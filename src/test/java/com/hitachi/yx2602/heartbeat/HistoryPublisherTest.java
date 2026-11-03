package com.hitachi.yx2602.heartbeat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class HistoryPublisherTest {
    @TempDir Path directory;

    private static String read(Path file) throws Exception {
        return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
    }

    private void localConfig(Path source, Path target, Integer count) throws Exception {
        Properties properties = ReaderConfigTest.properties();
        properties.setProperty("ignite.history.config.path", target.toUri().toString());
        properties.setProperty("ignite.history.refresh.interval.ms", "1000");
        if (count != null) properties.setProperty("max.records", count.toString());
        // These values must never be copied to the generated shared file.
        properties.setProperty("ignite.username", "private-user");
        properties.setProperty("ignite.password", "private-password");
        Path temporary = source.resolveSibling("local.next");
        try (java.io.Writer writer = Files.newBufferedWriter(temporary, StandardCharsets.UTF_8)) {
            properties.store(writer, null);
        }
        Files.move(temporary, source, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                java.nio.file.StandardCopyOption.ATOMIC_MOVE);
    }

    @Test void generatesPolicyUpdatesOnlyCountAndPreservesVersionAcrossPublisherRestart() throws Exception {
        Path source = directory.resolve("local.properties"), target = directory.resolve("shared/history.properties");
        localConfig(source, target, 10);
        long version;
        try (IgniteConfig.HistoryPublisher publisher = new IgniteConfig.HistoryPublisher(source)) {
            publisher.sync();
            IgniteConfig.HistoryPolicy first = IgniteConfig.HistorySettings.readPolicy(target.toUri().toString());
            assertEquals(10, first.maxRecords);
            assertTrue(first.version > 0);
            String generated = read(target);
            assertFalse(generated.contains("private-user"));
            assertFalse(generated.contains("private-password"));
            publisher.sync();
            assertEquals(generated, read(target), "unchanged count does not allocate a new version");
            localConfig(source, target, 20);
            publisher.sync();
            IgniteConfig.HistoryPolicy second = IgniteConfig.HistorySettings.readPolicy(target.toUri().toString());
            assertEquals(20, second.maxRecords);
            assertTrue(second.version > first.version);
            version = second.version;
        }
        localConfig(source, target, 5);
        try (IgniteConfig.HistoryPublisher restarted = new IgniteConfig.HistoryPublisher(source)) {
            restarted.sync();
            IgniteConfig.HistoryPolicy third = IgniteConfig.HistorySettings.readPolicy(target.toUri().toString());
            assertEquals(5, third.maxRecords);
            assertTrue(third.version > version);
        }
    }

    @Test void incompleteInvalidAndRedirectedLocalConfigurationCannotOverwriteLastValidPolicy() throws Exception {
        Path source = directory.resolve("local.properties"), target = directory.resolve("history.properties");
        localConfig(source, target, 10);
        try (IgniteConfig.HistoryPublisher publisher = new IgniteConfig.HistoryPublisher(source)) {
            publisher.sync();
            String original = read(target);
            for (Integer invalid : new Integer[]{null, 0, 1001}) {
                localConfig(source, target, invalid);
                assertThrows(IllegalArgumentException.class, publisher::sync);
                assertEquals(original, read(target));
            }
            Path other = directory.resolve("other.properties");
            localConfig(source, other, 5);
            assertThrows(IllegalArgumentException.class, publisher::sync);
            assertFalse(Files.exists(other));
            Files.delete(source);
            assertThrows(java.io.IOException.class, publisher::sync);
            assertEquals(original, read(target));
        }
    }

    @Test void backgroundPublisherObservesOnlyLocalEditsAndReaderReloadsGeneratedVersion() throws Exception {
        Path source = directory.resolve("local.properties"), target = directory.resolve("history.properties");
        localConfig(source, target, 10);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (IgniteConfig.HistoryPublisher publisher = new IgniteConfig.HistoryPublisher(source)) {
            publisher.sync();
            try (IgniteConfig.HistorySettings reader = new IgniteConfig.HistorySettings(ReaderConfig.load(source).ignite)) {
                Thread watcher = new Thread(() -> {
                    try { publisher.watch(); }
                    catch (InterruptedException expected) { Thread.currentThread().interrupt(); }
                    catch (Throwable error) { failure.set(error); }
                });
                watcher.start();
                try {
                    localConfig(source, target, 5);
                    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(6);
                    while (reader.current().maxRecords != 5 && System.nanoTime() < deadline) Thread.sleep(20);
                    assertEquals(5, reader.current().maxRecords);
                    assertNull(failure.get());
                } finally {
                    watcher.interrupt(); watcher.join(3000);
                    assertFalse(watcher.isAlive());
                }
            }
        }
    }

    @Test void rejectsDuplicatePublisherAndCanStartAgainAfterClose() throws Exception {
        Path source = directory.resolve("local.properties"), target = directory.resolve("history.properties");
        localConfig(source, target, 10);
        try (IgniteConfig.HistoryPublisher first = new IgniteConfig.HistoryPublisher(source)) {
            assertThrows(java.nio.channels.OverlappingFileLockException.class, () -> new IgniteConfig.HistoryPublisher(source));
            first.sync();
        }
        try (IgniteConfig.HistoryPublisher next = new IgniteConfig.HistoryPublisher(source)) { next.sync(); }
    }

    @Test void rejectsMissingCountSelfOverwriteAndConflictingAliases() throws Exception {
        Path source = directory.resolve("local.properties"), target = directory.resolve("history.properties");
        localConfig(source, target, null);
        assertThrows(IllegalArgumentException.class, () -> new IgniteConfig.HistoryPublisher(source));
        localConfig(source, source, 10);
        assertThrows(IllegalArgumentException.class, () -> new IgniteConfig.HistoryPublisher(source));
        Properties p = ReaderConfigTest.properties();
        p.setProperty("max.records", "5"); p.setProperty("ignite.history.max.records", "10");
        assertThrows(IllegalArgumentException.class, () -> ReaderConfig.from(p, key -> null));
    }
}
