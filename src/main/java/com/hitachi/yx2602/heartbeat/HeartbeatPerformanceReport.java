package com.hitachi.yx2602.heartbeat;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Read-only log statistics; runs with Java 11 alone, independently of Flink and the collector. */
public final class HeartbeatPerformanceReport {
    private static final ZoneId ZONE = ZoneId.of("Asia/Shanghai");
    private static final DateTimeFormatter LOG_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS");
    private static final DateTimeFormatter DISPLAY = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss XXX").withZone(ZONE);
    private static final Pattern FIELD = Pattern.compile("([a-zA-Z_][a-zA-Z_0-9.-]*)=([^\\s]+)");
    private static final Set<String> STAGES = new HashSet<>(Arrays.asList("validation", "split", "ordering", "ignite_sink"));

    private HeartbeatPerformanceReport() { }

    public static void main(String[] args) throws Exception {
        Options options = Options.parse(args);
        do {
            try {
                String report = analyze(options.inputs, options.minutes, options.historical, Instant.now(), options.simple);
                Path target = options.output.toAbsolutePath().normalize();
                for (Path input : options.inputs) {
                    if (target.equals(input.toAbsolutePath().normalize())
                            || (Files.exists(target) && Files.isSameFile(target, input))) {
                        throw new IllegalArgumentException("报告不能覆盖输入日志");
                    }
                }
                Files.createDirectories(target.getParent());
                Path temporary = Files.createTempFile(target.getParent(), ".heartbeat-performance-", ".tmp");
                try {
                    Files.writeString(temporary, report, StandardCharsets.UTF_8);
                    try { Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING); }
                    catch (java.nio.file.AtomicMoveNotSupportedException unavailable) {
                        Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
                    }
                } finally { Files.deleteIfExists(temporary); }
                if (options.simple) System.out.print(report);
                System.out.println(DISPLAY.format(Instant.now()) + " report=" + target);
            } catch (IOException failure) {
                if (!options.watch) throw failure;
                System.err.println(DISPLAY.format(Instant.now()) + " report_retry=" + failure.getMessage());
            }
            if (options.watch) Thread.sleep(30000);
        } while (options.watch && !Thread.currentThread().isInterrupted());
    }

    static String analyze(List<Path> inputs, int minutes, boolean historical, Instant now) throws IOException {
        return analyze(inputs, minutes, historical, now, false);
    }

    static String analyze(List<Path> inputs, int minutes, boolean historical, Instant now, boolean simple) throws IOException {
        if (minutes < 1 || minutes > 1440) throw new IllegalArgumentException("minutes须为1～1440");
        // First pass selects the newest job and log-time anchor without keeping old metric maps in memory.
        Sample[] latest = {null};
        scan(inputs, sample -> {
            if (sample.perf && (latest[0] == null || sample.time.isAfter(latest[0].time))) latest[0] = sample;
        });
        StringBuilder out = new StringBuilder("# 心跳链路性能统计\n\n生成时间：").append(DISPLAY.format(now)).append("\n\n");
        if (latest[0] == null) return out.append("没有可识别的完整性能摘要。请检查日志同步、监控开关及输入文件。\n").toString();
        Instant end = historical ? latest[0].time : now;
        Instant start = end.minusSeconds(minutes * 60L);
        String job = latest[0].job;
        List<Sample> samples = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        scan(inputs, sample -> {
            if (job.equals(sample.job) && !sample.time.isBefore(start) && !sample.time.isAfter(end)
                    && seen.add(sample.signature())) samples.add(sample);
        });
        out.append("模式：").append(historical ? "历史回看（窗口结束于日志最后摘要，不代表当前运行）" : "当前时间窗口")
                .append("；Job：`").append(job).append("`。\n\n")
                .append("请求范围：").append(DISPLAY.format(start)).append(" ～ ").append(DISPLAY.format(end))
                .append("；输入中最后性能摘要：").append(DISPLAY.format(latest[0].time)).append("。\n\n");
        long age = now.toEpochMilli() - latest[0].time.toEpochMilli();
        if (age > 120000) out.append("**日志已超过2分钟未更新：距生成时刻约").append(f(age / 1000.0))
                .append("秒。旧样本不能证明当前健康；检查作业、采集器和监控周期。**\n\n");
        if (age < -60000) out.append("**日志时间领先当前时钟，请核对时区及节点时钟。**\n\n");
        if (samples.stream().noneMatch(sample -> sample.perf)) {
            return out.append("该窗口没有性能样本，不能按0吞吐或健康状态解读。历史文件可加 `--as-of-latest`。\n").toString();
        }

        Map<String, Integer> attempts = new HashMap<>();
        for (Sample sample : samples) attempts.merge(sample.stage + "/" + sample.task, sample.attempt, Math::max);
        int omitted = 0;
        Map<String, List<Sample>> stages = new HashMap<>();
        List<Sample> checkpoints = new ArrayList<>();
        for (Sample sample : samples) {
            if (sample.attempt != attempts.get(sample.stage + "/" + sample.task)) { omitted++; continue; }
            if (sample.perf) stages.computeIfAbsent(sample.stage, ignored -> new ArrayList<>()).add(sample);
            else checkpoints.add(sample);
        }
        if (omitted > 0) out.append("窗口内有恢复尝试：已排除").append(omitted)
                .append("条较旧attempt记录，仅统计各子任务最后一次尝试；跨尝试总量未合并。\n\n");
        Stats read = stats(stages, "validation"), split = stats(stages, "split");
        Stats order = stats(stages, "ordering"), sink = stats(stages, "ignite_sink");
        if (simple) {
            simpleRow(out, "消费平均", "原始消息", read, "input", minutes);
            simpleRow(out, "解析平均", "单梯消息", split, "parsed", minutes);
            simpleRow(out, "Ignite写入平均", "单梯更新", sink, "committed", minutes);
            return out.append("最高值取同一采样窗口全部子任务的合计速率；不是各子任务各自最高值相加，也不是单秒瞬时峰值。\n\n")
                    .append("三个指标均合计已覆盖的全部Topic和子任务。原始消息可拆多梯并经过过滤，三个数字不要求相等。\n").toString();
        }
        out.append("## 吞吐与覆盖\n\n| 阶段 | 单位 | 输入/秒 | 输出/秒 | 摘要数 | 子任务覆盖 | 实际样本覆盖秒 |\n")
                .append("| --- | --- | ---: | ---: | ---: | --- | ---: |\n");
        stageRow(out, "Kafka读取/校验", "原始消息", read, "input", "accepted");
        stageRow(out, "分梯解析", "输入原始消息，输出单梯", split, "input", "parsed");
        stageRow(out, "新旧判断", "单梯消息", order, "input", "accepted");
        stageRow(out, "Ignite写入", "单梯更新，非物理行数", sink, "batch_records", "committed");
        out.append("\n速率为该阶段全部已覆盖子任务的计数之和÷实际样本时间跨度，不再除以并行度。")
                .append("窗口可能跨越请求边界；缺失子任务或采样断档会低估全作业吞吐。\n\n");
        for (String name : new String[]{"validation", "split", "ordering", "ignite_sink"}) {
            Stats s = stats(stages, name);
            if (s.records.isEmpty() || s.coverage() < s.parallelism()) out.append("**覆盖不足：").append(name)
                    .append(" 仅有 ").append(s.coverage()).append('/').append(s.parallelism())
                    .append(" 个子任务，不是完整全作业统计。**\n\n");
        }
        out.append("## 过滤与异常计数\n\n| 指标 | 次数 | 说明 |\n| --- | ---: | --- |\n");
        countRow(out, "读取校验拒绝", read, "rejected", "原始消息");
        countRow(out, "解析入口过期", split, "expired", "原始消息；占解析输入 " + percent(split.sum("expired"), split.sum("input")));
        countRow(out, "端口解析拒绝", split, "rejected", "按端口计数，不能当成同等数量原始消息");
        countRow(out, "新旧判断过期", order, "expired", "单梯消息");
        countRow(out, "同梯重复", order, "duplicate", "单梯消息");
        countRow(out, "同梯旧版本", order, "older", "单梯消息");
        countRow(out, "数据库跳过", sink, "skipped", "CreatedDate不比库内更新，非成功有效写入");
        countRow(out, "客户端失败", sink, "client_failures", "可能与下列计数描述同一次失败，不能相加为独立事故数");
        countRow(out, "事务失败", sink, "transaction_failures", "失败尝试计数");
        countRow(out, "重试", sink, "retries", "重试尝试计数");

        out.append("\n## 计算、等待与延迟\n\n| 阶段/指标 | 加权平均ms | 最大观测ms |\n| --- | ---: | ---: |\n");
        timingRow(out, "JSON/时间校验计算", read, "validate");
        timingRow(out, "分梯解析计算", split, "split");
        timingRow(out, "新旧判断计算", order, "order");
        timingRow(out, "新旧判断向下游输出（含等待）", order, "emit");
        timingRow(out, "Sink入队调用", sink, "enqueue");
        timingRow(out, "入库前缓冲等待", sink, "buffer_wait");
        timingRow(out, "每批写入总耗时", sink, "batch_write");
        timingRow(out, "每次读取最新状态（含锁/网络）", sink, "latest_read");
        timingRow(out, "每次批量putAll", sink, "bulk_write");
        timingRow(out, "每次事务提交", sink, "commit");
        timingRow(out, "读取时Kafka时间戳年龄", read, "kafka_age");
        timingRow(out, "读取时CreatedDate年龄", read, "created_age");
        timingRow(out, "批次返回时CreatedDate年龄", sink, "created_age_after_batch");
        out.append("\n平均值按total_ms÷samples计算；无调用样本显示—，不是0延迟。最大值来自摘要max，不能据此计算P95/P99。")
                .append("Kafka年龄可能含生产者延迟，不等于精确消费LAG；批次年龄包含被数据库跳过的消息。\n\n");
        out.append("每批平均记录数：").append(ratio(sink.sum("batch_records"), sink.sum("batches")))
                .append("；每批平均逐梯读取次数：").append(ratio(sink.sum("latest_read_samples"), sink.sum("batches")))
                .append("。批次累计耗时中，读取占 ").append(percent(sink.sum("latest_read_total_ms"), sink.sum("batch_write_total_ms")))
                .append("，putAll占 ").append(percent(sink.sum("bulk_write_total_ms"), sink.sum("batch_write_total_ms")))
                .append("，提交占 ").append(percent(sink.sum("commit_total_ms"), sink.sum("batch_write_total_ms")))
                .append("。这些是客户端墙钟耗时，非CPU占用；不能与包含它们的批次总耗时重复相加。\n\n");

        out.append("## 各路Sink\n\n| 子任务/attempt | 有效更新/秒 | 最新队列/容量 | 队列均值 | 满队列快照/总数 | 批次均值ms | 写调用占采样时间 | 最新writer_failed | 最新进行中写批次ms | 最后摘要时间 |\n")
                .append("| --- | ---: | --- | ---: | --- | ---: | ---: | ---: | ---: | --- |\n");
        Map<Integer, List<Sample>> tasks = new TreeMap<>();
        for (Sample sample : sink.records) tasks.computeIfAbsent(sample.task, ignored -> new ArrayList<>()).add(sample);
        for (Map.Entry<Integer, List<Sample>> entry : tasks.entrySet()) {
            Stats s = new Stats(entry.getValue()); Sample last = s.latest();
            long full = s.records.stream().filter(r -> r.values.containsKey("queued") && r.value("queue_capacity") > 0
                    && r.value("queued") >= r.value("queue_capacity")).count();
            long gaugeSamples = s.records.stream().filter(r -> r.values.containsKey("queued")).count();
            out.append('|').append(entry.getKey()).append('/').append(last.parallelism).append(" attempt=").append(last.attempt)
                    .append('|').append(s.rate("committed")).append('|').append(number(last, "queued")).append('/')
                    .append(number(last, "queue_capacity")).append('|').append(ratio(s.sum("queued"), gaugeSamples))
                    .append('|').append(full).append('/').append(gaugeSamples).append('|').append(s.average("batch_write"))
                    .append('|').append(percent(s.sum("batch_write_total_ms"), s.sum("window_ms")))
                    .append('|').append(number(last, "writer_failed")).append('|').append(number(last, "batch_write_active_ms"))
                    .append('|').append(DISPLAY.format(last.time)).append("|\n");
        }
        out.append("\n队列是周期采样，不代表每秒状态；写调用比例包含网络与阻塞，不能视为Flink busy或CPU指标。\n\n");
        checkpointRows(out, checkpoints);
        out.append("## Kafka分区最后观察位点\n\n| Topic/分区 | 校验阶段最后观察offset | 样本时间 |\n| --- | ---: | --- |\n");
        Map<String, Sample> offsets = new TreeMap<>();
        for (Sample sample : read.records) for (String key : sample.values.keySet()) {
            if (key.startsWith("observed_offset_") && (!offsets.containsKey(key) || sample.time.isAfter(offsets.get(key).time))) {
                offsets.put(key, sample);
            }
        }
        offsets.forEach((key, sample) -> out.append('|').append(offsetLabel(key)).append('|')
                .append(number(sample, key)).append('|').append(DISPLAY.format(sample.time)).append("|\n"));
        out.append("\n上述offset不是Broker末尾或已提交位点。精确LAG需另对照Kafka末尾、Source当前和组提交位点；本工具不连接Kafka或Ignite。")
                .append("不同阶段单位与过滤规则不同，不能简单用Source数量减Sink数量作为丢失数。\n");
        return out.toString();
    }

    private static void checkpointRows(StringBuilder out, List<Sample> records) {
        Set<String> completed = new HashSet<>(), aborted = new HashSet<>();
        Map<String, Map<String, Instant>> events = new HashMap<>();
        for (Sample sample : records) {
            if ("completed".equals(sample.event)) completed.add(sample.checkpoint);
            if ("aborted".equals(sample.event)) aborted.add(sample.checkpoint);
            String key = sample.task + "/" + sample.attempt + "/" + sample.checkpoint;
            events.computeIfAbsent(key, ignored -> new HashMap<>()).put(sample.event, sample.time);
        }
        double total = 0, max = 0; int count = 0;
        for (Map<String, Instant> e : events.values()) if (e.containsKey("flush_started") && e.containsKey("flush_finished")) {
            long ms = e.get("flush_finished").toEpochMilli() - e.get("flush_started").toEpochMilli();
            if (ms >= 0) { total += ms; max = Math.max(max, ms); count++; }
        }
        out.append("## 检查点\n\n完成通知涉及 ").append(completed.size()).append(" 个检查点ID，中止通知涉及 ")
                .append(aborted.size()).append(" 个ID；配对的Sink刷写等待 ").append(count).append(" 次，平均 ")
                .append(ratio(total, count)).append("ms，最大 ").append(count == 0 ? "—" : f(max)).append("ms。\n\n")
                .append("完成通知按ID去重，可能只覆盖部分子任务；不是完整检查点成功率。刷写等待不等于从全局trigger计算的检查点总耗时，也不证明Kafka提交请求已被Broker确认。\n\n");
    }

    private static void stageRow(StringBuilder out, String label, String unit, Stats stats, String input, String output) {
        out.append('|').append(label).append('|').append(unit).append('|').append(stats.rate(input)).append('|')
                .append(stats.rate(output)).append('|').append(stats.records.size()).append('|').append(stats.coverage())
                .append('/').append(stats.parallelism()).append('|').append(f(stats.seconds())).append("|\n");
    }

    private static void simpleRow(StringBuilder out, String label, String unit, Stats stats, String counter, int minutes) {
        out.append(label).append("：").append(stats.rate(counter)).append(" 条").append(unit).append("/秒");
        if (stats.records.isEmpty()) {
            out.append("（没有样本，不能按0吞吐解读）\n\n");
            return;
        }
        out.append("（已采集 ").append(integer(stats.sum(counter))).append(" 条，覆盖 ").append(f(stats.seconds()))
                .append(" 秒，子任务 ").append(stats.coverage()).append('/').append(stats.parallelism()).append("）");
        Peak peak = stats.peak(counter);
        if (peak == null) out.append("；最高：—（缺少完整的并行采样窗口）\n\n");
        else out.append("；最高：").append(f(peak.rate)).append(" 条").append(unit).append("/秒（约")
                .append(f(peak.windowMs / 1000)).append("秒采样窗口）\n\n");
        if (stats.coverage() < stats.parallelism()) out.append("提示：").append(label).append("缺少子任务样本，不能代表全作业。\n\n");
        if (stats.seconds() < minutes * 60 - 1) out.append("提示：").append(label).append("样本不足").append(minutes)
                .append("分钟；条数、平均值和最高值仅代表已采集区间，不是完整")
                .append(minutes).append("分钟指标。未采集区间不按0计数，也不外推补齐。\n\n");
    }

    private static void countRow(StringBuilder out, String label, Stats stats, String key, String note) {
        out.append('|').append(label).append('|').append(stats.records.isEmpty() ? "—" : integer(stats.sum(key)))
                .append('|').append(note).append("|\n");
    }

    private static void timingRow(StringBuilder out, String label, Stats stats, String key) {
        out.append('|').append(label).append('|').append(stats.average(key)).append('|')
                .append(stats.sum(key + "_samples") == 0 ? "—" : f(stats.max(key + "_max_ms"))).append("|\n");
    }

    private static Stats stats(Map<String, List<Sample>> stages, String name) { return new Stats(stages.getOrDefault(name, new ArrayList<>())); }
    private static String number(Sample sample, String key) { return sample.values.containsKey(key) ? integer(sample.value(key)) : "—"; }
    private static String offsetLabel(String key) {
        String name = key.substring("observed_offset_".length());
        int partition = name.lastIndexOf("_p");
        return partition < 0 ? "旧日志未注明Topic/" + name : name.substring(0, partition) + "/p" + name.substring(partition + 2);
    }
    private static String integer(double value) { return String.format(Locale.ROOT, "%.0f", value); }
    private static String f(double value) { return String.format(Locale.ROOT, "%.3f", value); }
    private static String ratio(double numerator, double denominator) { return denominator > 0 ? f(numerator / denominator) : "—"; }
    private static String percent(double numerator, double denominator) { return denominator > 0 ? f(numerator / denominator * 100) + "%" : "—"; }

    private static void scan(List<Path> paths, Consumer<Sample> action) throws IOException {
        for (Path path : paths) {
            boolean terminated;
            try (SeekableByteChannel channel = Files.newByteChannel(path)) {
                long size = channel.size(); ByteBuffer end = ByteBuffer.allocate(1);
                if (size == 0) continue;
                channel.position(size - 1); channel.read(end); terminated = end.get(0) == '\n';
            }
            try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
                String line = reader.readLine();
                while (line != null) {
                    String next = reader.readLine();
                    if (next != null || terminated) {
                        Sample sample = Sample.parse(line);
                        if (sample != null) action.accept(sample);
                    }
                    line = next;
                }
            }
        }
    }

    static final class Sample {
        Instant time; String job, stage, event = "", checkpoint = "";
        int task, parallelism, attempt; boolean perf;
        final Map<String, Double> values = new TreeMap<>();

        double value(String key) { return values.getOrDefault(key, 0.0); }

        String signature() {
            // Millisecond precision also deduplicates the same event in ISO and ordinary Flink logs.
            return time.toEpochMilli() + "/" + job + "/" + stage + "/" + task + "/" + attempt + "/" + event + "/" + checkpoint + "/" + values;
        }

        static Sample parse(String line) {
            int marker = line.indexOf("[HEARTBEAT-PERF]"); boolean perf = marker >= 0;
            if (!perf) marker = line.indexOf("[HEARTBEAT-CHECKPOINT]");
            if (marker < 0) return null;
            try {
                Sample sample = new Sample(); sample.perf = perf;
                String prefix = line.startsWith("\uFEFF") ? line.substring(1) : line;
                if (prefix.length() > 10 && prefix.charAt(10) == 'T') {
                    sample.time = ZonedDateTime.parse(prefix.substring(0, prefix.indexOf(' '))).toInstant();
                } else sample.time = LocalDateTime.parse(prefix.substring(0, 23), LOG_TIME).atZone(ZONE).toInstant();
                Map<String, String> fields = new HashMap<>();
                Matcher matcher = FIELD.matcher(line.substring(marker));
                while (matcher.find()) fields.put(matcher.group(1), matcher.group(2));
                sample.job = fields.get("job"); sample.stage = fields.get("stage");
                if (sample.job == null || !STAGES.contains(sample.stage)) return null;
                String[] task = fields.get("subtask").split("/");
                sample.task = Integer.parseInt(task[0]); sample.parallelism = Integer.parseInt(task[1]);
                sample.attempt = Integer.parseInt(fields.get("attempt"));
                if (sample.task < 0 || sample.parallelism <= sample.task || sample.attempt < 0) return null;
                for (Map.Entry<String, String> field : fields.entrySet()) {
                    if (Arrays.asList("job", "stage", "subtask", "attempt", "event", "event_id", "checkpoint_id").contains(field.getKey())) continue;
                    double value = Double.parseDouble(field.getValue());
                    if (!Double.isFinite(value) || value < 0) return null;
                    sample.values.put(field.getKey(), value);
                }
                if (perf && sample.value("window_ms") <= 0) return null;
                sample.event = fields.getOrDefault("event", ""); sample.checkpoint = fields.getOrDefault("checkpoint_id", "");
                if (!perf && (sample.event.isEmpty() || sample.checkpoint.isEmpty())) return null;
                return sample;
            } catch (RuntimeException malformed) { return null; }
        }
    }

    private static final class Stats {
        final List<Sample> records;
        Stats(List<Sample> records) { this.records = records; }
        double sum(String key) { return records.stream().mapToDouble(r -> r.value(key)).sum(); }
        double max(String key) { return records.stream().mapToDouble(r -> r.value(key)).max().orElse(0); }
        double seconds() {
            if (records.isEmpty()) return 0;
            double start = records.stream().mapToDouble(r -> r.time.toEpochMilli() - r.value("window_ms")).min().orElse(0);
            long end = records.stream().mapToLong(r -> r.time.toEpochMilli()).max().orElse(0);
            return Math.max(0.001, (end - start) / 1000);
        }
        String rate(String key) { return records.isEmpty() ? "—" : f(sum(key) / seconds()); }
        String average(String key) { return ratio(sum(key + "_total_ms"), sum(key + "_samples")); }
        int parallelism() { return records.stream().mapToInt(r -> r.parallelism).max().orElse(0); }
        int coverage() { return (int) records.stream().map(r -> r.task).distinct().count(); }
        Sample latest() { return records.stream().max(java.util.Comparator.comparing(r -> r.time)).orElseThrow(); }

        Peak peak(String counter) {
            if (records.isEmpty()) return null;
            double[] windows = records.stream().mapToDouble(r -> r.value("window_ms")).sorted().toArray();
            double typicalMs = windows[windows.length / 2];
            long anchor = records.stream().filter(r -> regularWindow(r, typicalMs))
                    .mapToLong(r -> r.time.toEpochMilli()).min().orElse(0);
            Map<Long, Map<Integer, Sample>> groups = new TreeMap<>();
            Set<Long> ambiguous = new HashSet<>();
            for (Sample sample : records) {
                if (!regularWindow(sample, typicalMs)) continue;
                // Round relative to this stage's first regular sample, not a wall-clock boundary.
                // Parallel task timers can straddle :00/:30 due to small startup/scheduling offsets.
                long slot = Math.round((sample.time.toEpochMilli() - anchor) / typicalMs);
                Map<Integer, Sample> group = groups.computeIfAbsent(slot, ignored -> new HashMap<>());
                if (group.putIfAbsent(sample.task, sample) != null) ambiguous.add(slot);
            }
            Peak best = null;
            for (Map.Entry<Long, Map<Integer, Sample>> entry : groups.entrySet()) {
                if (ambiguous.contains(entry.getKey()) || entry.getValue().size() != parallelism()) continue;
                List<Sample> group = new ArrayList<>(entry.getValue().values());
                long first = group.stream().mapToLong(r -> r.time.toEpochMilli()).min().orElse(0);
                long last = group.stream().mapToLong(r -> r.time.toEpochMilli()).max().orElse(0);
                if (last - first > typicalMs / 2) continue;
                double rate = group.stream().mapToDouble(r -> r.value(counter) * 1000 / r.value("window_ms")).sum();
                if (best == null || rate > best.rate) best = new Peak(rate, typicalMs);
            }
            return best;
        }

        private static boolean regularWindow(Sample sample, double typicalMs) {
            // Exclude short shutdown summaries and substantially delayed samples from period peaks.
            return sample.value("window_ms") >= typicalMs * 0.5 && sample.value("window_ms") <= typicalMs * 1.5;
        }
    }

    private static final class Peak {
        final double rate, windowMs;
        Peak(double rate, double windowMs) { this.rate = rate; this.windowMs = windowMs; }
    }

    static final class Options {
        final List<Path> inputs = new ArrayList<>();
        Path output = Paths.get("/home/xiong/projects/YX2602_FAULTRECOG/logs/heartbeat-performance.md");
        int minutes = 60; boolean watch, historical, simple;

        static Options parse(String[] args) {
            Options options = new Options();
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--input": options.inputs.add(Paths.get(requireValue(args, ++i))); break;
                    case "--output": options.output = Paths.get(requireValue(args, ++i)); break;
                    case "--minutes": options.minutes = Integer.parseInt(requireValue(args, ++i)); break;
                    case "--watch": options.watch = true; break;
                    case "--as-of-latest": options.historical = true; break;
                    case "--simple": options.simple = true; break;
                    default: throw new IllegalArgumentException("Usage: [--input LOG]... [--output REPORT.md] [--minutes 1..1440] [--simple] [--watch] [--as-of-latest]");
                }
            }
            if (options.minutes < 1 || options.minutes > 1440) throw new IllegalArgumentException("minutes须为1～1440");
            if (options.inputs.isEmpty()) options.inputs.add(Paths.get("/home/xiong/projects/YX2602_FAULTRECOG/logs/heartbeat-monitor.log"));
            return options;
        }
        private static String requireValue(String[] args, int index) {
            if (index >= args.length || args[index].startsWith("--")) throw new IllegalArgumentException("Missing option value");
            return args[index];
        }
    }
}
