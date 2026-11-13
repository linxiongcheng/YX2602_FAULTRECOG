package com.hitachi.yx2602.heartbeat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.*;

class HeartbeatLogFilesTest {
    @TempDir Path directory;
    private final TestClock clock = new TestClock("2026-11-13T04:00:00Z");

    @Test void sizeSegmentsPreserveUtf8CompleteLinesAndRestartNeverOverwrites() throws Exception {
        Path file = directory.resolve("heartbeat-monitor-application_1_1.log");
        HeartbeatLogFiles.DailyLog log = new HeartbeatLogFiles.DailyLog(file, 8, clock);
        log.append("甲"); log.append("乙"); log.append("丙");
        assertEquals("甲\n乙\n", Files.readString(segment(file, "2026-11-13", 1)));
        assertEquals("丙\n", Files.readString(file));
        log = new HeartbeatLogFiles.DailyLog(file, 8, clock);
        log.append("丁"); log.append("戊");
        assertEquals("丙\n丁\n", Files.readString(segment(file, "2026-11-13", 2)));
        assertEquals("戊\n", Files.readString(file));
        log.append("完整的很长一行");
        assertEquals("戊\n", Files.readString(segment(file, "2026-11-13", 3)));
        assertEquals("完整的很长一行\n", Files.readString(file));
        log.append("尾");
        assertEquals("完整的很长一行\n", Files.readString(segment(file, "2026-11-13", 4)));
    }

    @Test void rollsAtShanghaiMidnightEvenWhenThereIsNoInput() throws Exception {
        clock.now = Instant.parse("2026-11-12T15:59:59Z");
        Path file = directory.resolve("collector-application_1_1.log");
        HeartbeatLogFiles.DailyLog log = new HeartbeatLogFiles.DailyLog(file, 1000, clock);
        log.append("before midnight");
        clock.now = Instant.parse("2026-11-12T16:00:00Z");
        log.rollForToday();
        assertEquals("before midnight\n", Files.readString(segment(file, "2026-11-12", 1)));
        assertFalse(Files.exists(file));
        log.append("after midnight");
        assertEquals("after midnight\n", Files.readString(file));
    }

    @Test void archivesExistingOversizedLegacyFileWithoutDiscardingIt() throws Exception {
        Path file = directory.resolve("heartbeat-monitor-application_1_1.log");
        Files.writeString(file, "existing oversized data\n");
        Files.setLastModifiedTime(file, FileTime.from(Instant.parse("2026-11-12T02:00:00Z")));
        new HeartbeatLogFiles.DailyLog(file, 4, clock).append("new");
        assertEquals("existing oversized data\n", Files.readString(segment(file, "2026-11-12", 1)));
        assertEquals("new\n", Files.readString(file));
    }

    @Test void retainsExactlyTodayAndPreviousSixDaysAcrossApplications() throws Exception {
        Path old = write("heartbeat-monitor-application_1_1.log.2026-11-06.000001", clock.instant());
        Path boundary = write("heartbeat-monitor-application_1_2.log.2026-11-07.000001", Instant.EPOCH);
        Path today = write("collector-application_1_3.log.2026-11-13.000001", Instant.EPOCH);
        Path legacy = write("heartbeat-monitor-application_1_4.log", Instant.parse("2026-11-06T15:59:59Z"));
        Path legacyBoundary = write("collector-application_1_5.log.1", Instant.parse("2026-11-06T16:00:00Z"));
        Path bootstrap = write("collector-application_1_4.log.bootstrap.2026-11-06.123", clock.instant());
        Path submit = write("submit-20261106-120000-123.log", Instant.parse("2026-11-06T04:00:00Z"));
        HeartbeatLogFiles.Cleaner cleaner = new HeartbeatLogFiles.Cleaner(directory, 7, clock);
        assertEquals(4, cleaner.clean());
        for (Path path : new Path[]{old, legacy, bootstrap, submit}) assertFalse(Files.exists(path));
        for (Path path : new Path[]{boundary, today, legacyBoundary}) assertTrue(Files.exists(path));
    }

    @Test void neverDeletesStateUnknownFilesDirectoriesOrActiveCurrentLogs() throws Exception {
        String[] keep = {"collector-application_1_1.pid", ".heartbeat-monitor-application_1_1.offsets.json",
                ".heartbeat-monitor-application_1_1.offsets.json.lock", "heartbeat-monitor.log",
                "other.log", "submit-important.log", "heartbeat-monitor-application_1_1.log.not-a-segment",
                "heartbeat-monitor-application_1_1.log.2026-99-99.000001"};
        for (String name : keep) write(name, Instant.EPOCH);
        Path subdir = Files.createDirectory(directory.resolve("collector-application_1_4.log"));
        Files.writeString(subdir.resolve("important"), "keep");
        Path current = write("heartbeat-monitor-application_1_2.log", Instant.EPOCH);
        Path bootstrap = write("collector-application_1_2.log.bootstrap.2026-11-01.12", Instant.EPOCH);
        Path lock = directory.resolve(".heartbeat-monitor-application_1_2.offsets.json.lock");
        try (FileChannel owner = FileChannel.open(lock, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             FileLock ignored = owner.lock()) {
            assertEquals(0, new HeartbeatLogFiles.Cleaner(directory, 7, clock).clean());
            assertTrue(Files.exists(current)); assertTrue(Files.exists(bootstrap));
        }
        assertEquals(2, new HeartbeatLogFiles.Cleaner(directory, 7, clock).clean());
        for (String name : keep) assertTrue(Files.exists(directory.resolve(name)), name);
        assertTrue(Files.exists(subdir.resolve("important")));
    }

    @Test void cleanIfDueRunsHourlyAndImmediatelyAfterCalendarDayChanges() throws Exception {
        HeartbeatLogFiles.Cleaner cleaner = new HeartbeatLogFiles.Cleaner(directory, 7, clock);
        assertEquals(0, cleaner.cleanIfDue());
        Path old = write("collector-application_1_1.log.2026-11-01.000001", Instant.EPOCH);
        assertEquals(0, cleaner.cleanIfDue()); assertTrue(Files.exists(old));
        clock.now = clock.now.plusSeconds(3600);
        assertEquals(1, cleaner.cleanIfDue());
        clock.now = Instant.parse("2026-11-13T15:59:59Z");
        cleaner.cleanIfDue();
        Path boundary = write("collector-application_1_1.log.2026-11-07.000001", clock.instant());
        clock.now = clock.now.plusSeconds(1);
        assertEquals(1, cleaner.cleanIfDue()); assertFalse(Files.exists(boundary));
    }

    @Test void rejectsInvalidLimits() {
        Path file = directory.resolve("log");
        assertThrows(IllegalArgumentException.class, () -> new HeartbeatLogFiles.DailyLog(file, 0, clock));
        assertThrows(IllegalArgumentException.class, () -> new HeartbeatLogFiles.Cleaner(directory, 0, clock));
        assertThrows(IllegalArgumentException.class, () -> new HeartbeatLogFiles.Cleaner(directory, 3651, clock));
    }

    private Path write(String name, Instant modified) throws Exception {
        Path path = directory.resolve(name);
        Files.writeString(path, "data\n"); Files.setLastModifiedTime(path, FileTime.from(modified));
        return path;
    }

    private Path segment(Path file, String day, int number) {
        return file.resolveSibling(file.getFileName() + "." + day + "." + String.format("%06d", number));
    }

    private static final class TestClock extends Clock {
        Instant now;
        TestClock(String instant) { now = Instant.parse(instant); }
        @Override public ZoneId getZone() { return ZoneId.of("UTC"); }
        @Override public Clock withZone(ZoneId zone) { return Clock.fixed(now, zone); }
        @Override public Instant instant() { return now; }
    }
}
