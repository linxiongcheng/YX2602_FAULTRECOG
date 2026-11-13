package com.hitachi.yx2602.heartbeat;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Clock;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Submission-node log files only: complete segments and calendar-day retention. */
final class HeartbeatLogFiles {
    static final ZoneId ZONE = ZoneId.of("Asia/Shanghai");
    private static final Pattern LOG = Pattern.compile(
            "^(heartbeat-monitor|collector)-(application_[0-9]+_[0-9]+)\\.log"
                    + "(?:\\.[0-9]+|\\.[0-9]{4}-[0-9]{2}-[0-9]{2}\\.[0-9]+"
                    + "|\\.bootstrap(?:\\.[0-9]{4}-[0-9]{2}-[0-9]{2}\\.[0-9]+|\\.legacy\\.[0-9]+\\.[0-9]+)?)?$");
    private static final Pattern DATED = Pattern.compile("\\.log(?:\\.bootstrap)?\\.([0-9]{4}-[0-9]{2}-[0-9]{2})\\.[0-9]+$");
    private static final Pattern SUBMIT = Pattern.compile("^submit-[0-9]{8}-[0-9]{6}-[0-9]+\\.log$");

    private HeartbeatLogFiles() { }

    static int retentionDays() {
        int value = Integer.parseInt(System.getProperty("heartbeat.collector.retention.days", "7"));
        if (value < 1 || value > 3650) throw new IllegalArgumentException("heartbeat.collector.retention.days must be 1..3650");
        return value;
    }

    static final class DailyLog {
        private final Path file;
        private final long maxBytes;
        private final Clock clock;

        DailyLog(Path file, long maxBytes) { this(file, maxBytes, Clock.systemUTC()); }

        DailyLog(Path file, long maxBytes, Clock clock) {
            if (maxBytes <= 0) throw new IllegalArgumentException("Log max bytes must be positive");
            this.file = file; this.maxBytes = maxBytes; this.clock = clock;
        }

        void rollForToday() throws IOException {
            if (!Files.exists(file, LinkOption.NOFOLLOW_LINKS)) return;
            BasicFileAttributes attrs = attributes(file);
            if (!attrs.isRegularFile()) throw new IOException("Current log must be a regular file: " + file);
            LocalDate written = attrs.lastModifiedTime().toInstant().atZone(ZONE).toLocalDate();
            if (!written.equals(LocalDate.now(clock.withZone(ZONE)))) archive(written);
        }

        void append(String line) throws IOException {
            byte[] bytes = (line + "\n").getBytes(java.nio.charset.StandardCharsets.UTF_8);
            rollForToday();
            long size = Files.exists(file) ? Files.size(file) : 0;
            if (size > 0 && (size >= maxBytes || bytes.length > maxBytes - size)) {
                archive(LocalDate.now(clock.withZone(ZONE)));
            }
            // No truncation: a single oversized line is kept whole in its own segment.
            Files.write(file, bytes, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            Files.setLastModifiedTime(file, java.nio.file.attribute.FileTime.from(clock.instant()));
        }

        private void archive(LocalDate day) throws IOException {
            String prefix = file.getFileName() + "." + day + ".";
            long last = 0;
            try (DirectoryStream<Path> entries = Files.newDirectoryStream(file.toAbsolutePath().getParent())) {
                for (Path entry : entries) {
                    String name = entry.getFileName().toString();
                    if (!name.startsWith(prefix)) continue;
                    String suffix = name.substring(prefix.length());
                    if (!suffix.matches("[0-9]+")) continue;
                    try { last = Math.max(last, Long.parseLong(suffix)); }
                    catch (NumberFormatException invalid) { throw new IOException("Log segment number is too large: " + name, invalid); }
                }
            }
            if (last == Long.MAX_VALUE) throw new IOException("Log segment numbers exhausted: " + file);
            Path archived = file.resolveSibling(prefix + String.format(java.util.Locale.ROOT, "%06d", last + 1));
            Files.move(file, archived); // Never overwrite a retained segment, including after a restart.
        }
    }

    static final class Cleaner {
        private final Path directory;
        private final int days;
        private final Clock clock;
        private long nextCleanup;
        private LocalDate lastDay;

        Cleaner(Path directory, int days) { this(directory, days, Clock.systemUTC()); }

        Cleaner(Path directory, int days, Clock clock) {
            if (days < 1 || days > 3650) throw new IllegalArgumentException("Retention days must be 1..3650");
            this.directory = directory; this.days = days; this.clock = clock;
        }

        int cleanIfDue() throws IOException {
            LocalDate today = LocalDate.now(clock.withZone(ZONE));
            if (today.equals(lastDay) && clock.millis() < nextCleanup) return 0;
            int removed = clean();
            lastDay = today; nextCleanup = clock.millis() + 3600000L;
            return removed;
        }

        int clean() throws IOException {
            Files.createDirectories(directory);
            try (FileChannel channel = FileChannel.open(directory.resolve(".heartbeat-log-retention.lock"),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                try (FileLock lock = tryLock(channel)) {
                    if (lock == null) return 0;
                    LocalDate cutoff = LocalDate.now(clock.withZone(ZONE)).minusDays(days - 1L);
                    int removed = 0;
                    try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory)) {
                        for (Path path : entries) {
                            String name = path.getFileName().toString();
                            Matcher log = LOG.matcher(name);
                            boolean submit = SUBMIT.matcher(name).matches();
                            if (!log.matches() && !submit) continue;
                            if (!expired(path, cutoff)) continue;
                            // Completed daily segments are never appended again. Current/legacy files
                            // (including bootstrap output) require the owning collector to be stopped.
                            if (name.contains(".bootstrap") || !DATED.matcher(name).find()) {
                                if (submit) {
                                    if (expired(path, cutoff) && Files.deleteIfExists(path)) removed++;
                                } else {
                                    Path ownerLock = directory.resolve(".heartbeat-monitor-" + log.group(2) + ".offsets.json.lock");
                                    try (FileChannel owner = FileChannel.open(ownerLock, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                                         FileLock owned = tryLock(owner)) {
                                        if (owned != null && expired(path, cutoff) && Files.deleteIfExists(path)) removed++;
                                    }
                                }
                            } else if (Files.deleteIfExists(path)) removed++;
                        }
                    }
                    return removed;
                }
            }
        }

        private boolean expired(Path path, LocalDate cutoff) throws IOException {
            BasicFileAttributes attrs;
            try { attrs = attributes(path); }
            catch (java.nio.file.NoSuchFileException raced) { return false; }
            if (!attrs.isRegularFile()) return false; // Do not follow links or recurse into directories.
            Matcher date = DATED.matcher(path.getFileName().toString());
            LocalDate written;
            try {
                written = date.find() ? LocalDate.parse(date.group(1))
                        : attrs.lastModifiedTime().toInstant().atZone(ZONE).toLocalDate();
            } catch (java.time.DateTimeException invalidName) { return false; }
            return written.isBefore(cutoff);
        }
    }

    private static BasicFileAttributes attributes(Path file) throws IOException {
        return Files.readAttributes(file, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
    }

    private static FileLock tryLock(FileChannel channel) throws IOException {
        try { return channel.tryLock(); }
        catch (OverlappingFileLockException busy) { return null; }
    }
}
