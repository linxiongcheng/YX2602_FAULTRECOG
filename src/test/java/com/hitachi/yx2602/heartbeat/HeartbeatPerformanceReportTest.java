package com.hitachi.yx2602.heartbeat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class HeartbeatPerformanceReportTest {
    @TempDir Path directory;
    private static final Instant NOW = Instant.parse("2026-11-13T02:00:30Z");

    private String perf(String time, String job, String stage, String task, int attempt, String metrics) {
        return time + " [HEARTBEAT-PERF] stage=" + stage + " job=" + job + " subtask=" + task
                + " attempt=" + attempt + " window_ms=30000 " + metrics + "\n";
    }

    private Path log(String text) throws Exception {
        Path path = directory.resolve("input.log"); Files.writeString(path, text); return path;
    }

    @Test void sumsParallelThroughputAndWeightsTimingSamplesInsteadOfAveragingAverages() throws Exception {
        String iso = "2026-11-13T10:00:30+08:00[Asia/Shanghai]";
        String text = perf(iso, "job", "validation", "0/2", 0, "input=300 accepted=290")
                + perf(iso, "job", "validation", "1/2", 0, "input=600 accepted=590")
                + perf(iso, "job", "split", "0/2", 0, "input=300 parsed=400 expired=30 rejected=2")
                + perf(iso, "job", "split", "1/2", 0, "input=600 parsed=500 expired=60 rejected=1")
                + perf(iso, "job", "ordering", "0/2", 0, "input=400 accepted=250 duplicate=100 older=50")
                + perf(iso, "job", "ordering", "1/2", 0, "input=500 accepted=350 duplicate=100 older=50")
                + perf(iso, "job", "ignite_sink", "0/2", 0,
                "batch_records=250 committed=250 batches=10 latest_read_samples=1 latest_read_total_ms=10 latest_read_avg_ms=10 latest_read_max_ms=10 queued=100 queue_capacity=100 writer_failed=0")
                + perf(iso, "job", "ignite_sink", "1/2", 0,
                "batch_records=350 committed=350 batches=20 latest_read_samples=9 latest_read_total_ms=18 latest_read_avg_ms=2 latest_read_max_ms=2 queued=0 queue_capacity=100 writer_failed=0");
        String report = HeartbeatPerformanceReport.analyze(Collections.singletonList(log(text)), 15, false, NOW);
        assertTrue(report.contains("|Kafka读取/校验|原始消息|30.000|29.333|2|2/2|30.000|"), report);
        assertTrue(report.contains("|Ignite写入|单梯更新，非物理行数|20.000|20.000|2|2/2|30.000|"), report);
        assertTrue(report.contains("|每次读取最新状态（含锁/网络）|2.800|10.000|"), report);
        assertTrue(report.contains("|解析入口过期|90|原始消息；占解析输入 10.000%|"), report);
        assertTrue(report.contains("|端口解析拒绝|3|"));
        assertTrue(report.contains("|同梯重复|200|"));
        assertTrue(report.contains("|100/100|100.000|1/1|"));
    }

    @Test void deduplicatesCollectedAndOrdinaryFlinkCopiesAndIgnoresPartialFinalLine() throws Exception {
        String metrics = "committed=300 batch_records=300 batches=2";
        String iso = perf("2026-11-13T10:00:30.123456+08:00[Asia/Shanghai]", "job", "ignite_sink", "0/1", 0, metrics);
        String normal = perf("2026-11-13 10:00:30,123 INFO logger [] -", "job", "ignite_sink", "0/1", 0, metrics);
        String partial = perf("2026-11-13T10:01:00+08:00[Asia/Shanghai]", "job", "ignite_sink", "0/1", 0, "committed=9999").stripTrailing();
        Path path = log(iso + normal + partial);
        String report = HeartbeatPerformanceReport.analyze(Arrays.asList(path, path), 15, true, NOW);
        assertTrue(report.contains("|Ignite写入|单梯更新，非物理行数|10.000|10.000|1|1/1|30.000|"), report);
        assertFalse(report.contains("9999"));
    }

    @Test void currentModeDoesNotPresentOldLogsAsCurrentAndHistoricalModeIsExplicit() throws Exception {
        Path path = log(perf("2026-11-10T10:00:30+08:00[Asia/Shanghai]", "old", "ignite_sink", "0/1", 0, "committed=300"));
        String current = HeartbeatPerformanceReport.analyze(Collections.singletonList(path), 15, false, NOW);
        assertTrue(current.contains("该窗口没有性能样本"));
        assertTrue(current.contains("日志已超过2分钟未更新"));
        String historical = HeartbeatPerformanceReport.analyze(Collections.singletonList(path), 15, true, NOW);
        assertTrue(historical.contains("历史回看"));
        assertTrue(historical.contains("|Ignite写入|单梯更新，非物理行数|0.000|10.000|1|1/1|30.000|"));
    }

    @Test void isolatesNewestJobAndLatestAttemptWithoutSilentlyFillingMissingTasks() throws Exception {
        String text = perf("2026-11-13T09:59:00+08:00[Asia/Shanghai]", "old", "ignite_sink", "0/1", 0, "committed=9999")
                + perf("2026-11-13T10:00:00+08:00[Asia/Shanghai]", "new", "ignite_sink", "0/3", 0, "committed=900")
                + perf("2026-11-13T10:00:30+08:00[Asia/Shanghai]", "new", "ignite_sink", "0/3", 1, "committed=300");
        String report = HeartbeatPerformanceReport.analyze(Collections.singletonList(log(text)), 15, false, NOW);
        assertTrue(report.contains("Job：`new`"));
        assertTrue(report.contains("已排除1条较旧attempt记录"));
        assertTrue(report.contains("|Ignite写入|单梯更新，非物理行数|0.000|10.000|1|1/3|30.000|"));
        assertTrue(report.contains("覆盖不足：ignite_sink 仅有 1/3"));
    }

    @Test void pairsCheckpointFlushesAndDeduplicatesGlobalCompletionNotifications() throws Exception {
        String text = perf("2026-11-13T10:00:30+08:00[Asia/Shanghai]", "job", "ignite_sink", "0/2", 0, "committed=30");
        for (String task : Arrays.asList("0/2", "1/2")) {
            text += "2026-11-13T10:00:28+08:00[Asia/Shanghai] [HEARTBEAT-CHECKPOINT] stage=ignite_sink job=job subtask=" + task + " attempt=0 event=flush_started checkpoint_id=1\n";
            text += "2026-11-13T10:00:29+08:00[Asia/Shanghai] [HEARTBEAT-CHECKPOINT] stage=ignite_sink job=job subtask=" + task + " attempt=0 event=flush_finished checkpoint_id=1\n";
            text += "2026-11-13T10:00:30+08:00[Asia/Shanghai] [HEARTBEAT-CHECKPOINT] stage=ignite_sink job=job subtask=" + task + " attempt=0 event=completed checkpoint_id=1\n";
        }
        String report = HeartbeatPerformanceReport.analyze(Collections.singletonList(log(text)), 15, false, NOW);
        assertTrue(report.contains("完成通知涉及 1 个检查点ID"));
        assertTrue(report.contains("配对的Sink刷写等待 2 次，平均 1000.000ms，最大 1000.000ms"));
    }

    @Test void usesActualWallTimeWithGapsAndRejectsInvalidSamplesOrArguments() throws Exception {
        String text = perf("2026-11-13T09:59:30+08:00[Asia/Shanghai]", "job", "ignite_sink", "0/1", 0, "committed=300")
                + perf("2026-11-13T10:00:30+08:00[Asia/Shanghai]", "job", "ignite_sink", "0/1", 0, "committed=300");
        String report = HeartbeatPerformanceReport.analyze(Collections.singletonList(log(text)), 15, false, NOW);
        assertTrue(report.contains("|Ignite写入|单梯更新，非物理行数|0.000|6.667|2|1/1|90.000|"));
        assertNull(HeartbeatPerformanceReport.Sample.parse(perf("bad", "job", "ignite_sink", "0/1", 0, "committed=1")));
        assertNull(HeartbeatPerformanceReport.Sample.parse(perf("2026-11-13T10:00:30Z", "job", "ignite_sink", "0/1", 0, "committed=NaN")));
        assertThrows(IllegalArgumentException.class, () -> HeartbeatPerformanceReport.Options.parse(new String[]{"--minutes", "0"}));
        assertThrows(IllegalArgumentException.class, () -> HeartbeatPerformanceReport.Options.parse(new String[]{"--input"}));
    }

    @Test void cliWritesReportAndRefusesToOverwriteItsInput() throws Exception {
        Path input = log(perf("2026-11-13T10:00:30+08:00[Asia/Shanghai]", "job", "ignite_sink", "0/1", 0, "committed=300"));
        Path output = directory.resolve("report.md");
        HeartbeatPerformanceReport.main(new String[]{"--input", input.toString(), "--output", output.toString(), "--as-of-latest"});
        assertTrue(Files.readString(output).contains("历史回看"));
        String before = Files.readString(input);
        assertThrows(IllegalArgumentException.class, () -> HeartbeatPerformanceReport.main(
                new String[]{"--input", input.toString(), "--output", input.toString()}));
        assertEquals(before, Files.readString(input));
    }

    @Test void reportsTopicAndPartitionSeparatelyAndReadsLegacyOffsetFields() throws Exception {
        String text = perf("2026-11-13T10:00:30+08:00[Asia/Shanghai]", "job", "validation", "0/1", 0,
                "input=300 accepted=300 observed_offset_heartbeat1_p0=10 observed_offset_heartbeat2_p0=20 "
                        + "observed_offset_heartbeat-v1.test_p0=40 observed_offset_p2=50");
        String report = HeartbeatPerformanceReport.analyze(Collections.singletonList(log(text)), 15, false, NOW);
        assertTrue(report.contains("|heartbeat1/p0|10|"), report);
        assertTrue(report.contains("|heartbeat2/p0|20|"), report);
        assertTrue(report.contains("|heartbeat-v1.test/p0|40|"), report);
        assertTrue(report.contains("|旧日志未注明Topic/p2|50|"), report);
    }

    @Test void defaultsToSixtyMinutesButHonorsExplicitWindow() {
        assertEquals(60, HeartbeatPerformanceReport.Options.parse(new String[]{"--simple"}).minutes);
        assertEquals(60, HeartbeatPerformanceReport.Options.parse(new String[]{}).minutes);
        assertEquals(10, HeartbeatPerformanceReport.Options.parse(new String[]{"--minutes", "10", "--simple"}).minutes);
        assertEquals(10, HeartbeatPerformanceReport.Options.parse(new String[]{"--simple", "--minutes", "10"}).minutes);
    }

    @Test void simpleModeSumsFiveMinuteTotalsAcrossTopicsAndTasksWithoutCountingSkippedAsWrites() throws Exception {
        StringBuilder text = new StringBuilder();
        for (int window = 0; window < 10; window++) {
            String time = java.time.ZonedDateTime.ofInstant(NOW.minusSeconds(window * 30L), java.time.ZoneId.of("Asia/Shanghai")).toString();
            for (int task = 0; task < 2; task++) {
                text.append(perf(time, "job", "validation", task + "/2", 0, "input=300 accepted=290"));
                text.append(perf(time, "job", "split", task + "/2", 0, "parsed=450"));
                text.append(perf(time, "job", "ignite_sink", task + "/2", 0, "committed=200 skipped=100 batch_records=300"));
            }
        }
        String report = HeartbeatPerformanceReport.analyze(Collections.singletonList(log(text.toString())), 5, false, NOW, true);
        assertTrue(report.contains("消费平均：20.000 条原始消息/秒（已采集 6000 条，覆盖 300.000 秒，子任务 2/2）"), report);
        assertTrue(report.contains("解析平均：30.000 条单梯消息/秒（已采集 9000 条，覆盖 300.000 秒，子任务 2/2）"), report);
        assertTrue(report.contains("Ignite写入平均：13.333 条单梯更新/秒（已采集 4000 条，覆盖 300.000 秒，子任务 2/2）"), report);
        assertFalse(report.contains("## 各路Sink"));
        assertFalse(report.contains("## 计算、等待与延迟"));
    }

    @Test void simpleModeExplainsShortMissingAndOldSamples() throws Exception {
        Path path = log(perf("2026-11-13T10:00:30+08:00[Asia/Shanghai]", "job", "ignite_sink", "0/3", 0, "committed=300"));
        String report = HeartbeatPerformanceReport.analyze(Collections.singletonList(path), 5, false, NOW, true);
        assertTrue(report.contains("消费平均：— 条原始消息/秒（没有样本，不能按0吞吐解读）"));
        assertTrue(report.contains("Ignite写入平均：10.000 条单梯更新/秒"));
        assertTrue(report.contains("样本不足5分钟"));
        assertTrue(report.contains("不是完整5分钟指标"));
        assertTrue(report.contains("未采集区间不按0计数，也不外推补齐"));
        assertTrue(report.contains("缺少子任务样本"));
        assertTrue(report.contains("最高：—（缺少完整的并行采样窗口）"));
        String old = HeartbeatPerformanceReport.analyze(Collections.singletonList(path), 5, false, NOW.plusSeconds(3600), true);
        assertTrue(old.contains("该窗口没有性能样本"));
        assertFalse(old.contains("Ignite写入平均：0"));
    }

    @Test void peakAggregatesConcurrentWindowsAndExcludesIncompleteOrShortWindows() throws Exception {
        StringBuilder text = new StringBuilder();
        for (int window = 0; window < 2; window++) {
            String time = NOW.minusSeconds(60 - window * 30).atZone(java.time.ZoneId.of("Asia/Shanghai")).toString();
            text.append(perf(time, "job", "validation", "0/2", 0, "input=" + (window == 0 ? 3000 : 0)));
            text.append(perf(time, "job", "validation", "1/2", 0, "input=" + (window == 0 ? 0 : 6000)));
        }
        String time = NOW.atZone(java.time.ZoneId.of("Asia/Shanghai")).toString();
        text.append(perf(time, "job", "validation", "0/2", 0, "input=9000"));
        text.append(perf(time, "job", "validation", "1/2", 0, "input=999999").replace("window_ms=30000", "window_ms=1000"));
        String report = HeartbeatPerformanceReport.analyze(Collections.singletonList(log(text.toString())), 60, false, NOW, true);
        assertTrue(report.contains("最高：200.000 条原始消息/秒（约30.000秒采样窗口）"), report);
        assertFalse(report.contains("最高：300.000"));
        assertTrue(report.contains("样本不足60分钟"));
    }

    @Test void peakAlignsParallelSamplesAcrossWallClockBoundariesAndKeepsValidZero() throws Exception {
        StringBuilder text = new StringBuilder();
        for (int window = 0; window < 2; window++) {
            for (int task = 0; task < 2; task++) {
                String time = NOW.plusMillis(window * 30000L + (task == 0 ? -200 : 200))
                        .atZone(java.time.ZoneId.of("Asia/Shanghai")).toString();
                text.append(perf(time, "job", "validation", task + "/2", 0, "input=" + (window == 0 ? 3000 : 6000)));
                text.append(perf(time, "job", "ignite_sink", task + "/2", 0, "committed=0"));
            }
        }
        String report = HeartbeatPerformanceReport.analyze(Collections.singletonList(log(text.toString())), 60, false, NOW.plusSeconds(60), true);
        assertTrue(report.contains("最高：400.000 条原始消息/秒"), report);
        assertTrue(report.contains("最高：0.000 条单梯更新/秒"), report);
    }
}
