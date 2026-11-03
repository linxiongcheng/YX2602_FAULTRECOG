package com.hitachi.yx2602.heartbeat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import static org.junit.jupiter.api.Assertions.*;

class HistorySettingsTest {
    @TempDir Path directory;

    private IgniteConfig config(Path file) {
        Properties p = ReaderConfigTest.properties();
        if (file != null) p.setProperty("ignite.history.config.path", file.toUri().toString());
        p.setProperty("ignite.history.max.records", "7");
        return ReaderConfig.from(p, key -> null).ignite;
    }

    private void write(Path file, String text) throws Exception {
        Files.write(file, text.getBytes(StandardCharsets.UTF_8));
    }

    @Test void defaultAndFixedCountRemainAvailableWithoutSharedFile() {
        assertEquals(10, ReaderConfig.from(ReaderConfigTest.properties(), key -> null).ignite.historyMaxRecords);
        try (IgniteConfig.HistorySettings settings = new IgniteConfig.HistorySettings(config(null))) {
            assertEquals(7, settings.current().maxRecords);
            assertEquals(0, settings.current().version);
        }
    }

    @Test void reloadsIncreasingVersionsAndKeepsLastGoodValueOnBadOrMissingFiles() throws Exception {
        Path file = directory.resolve("history.properties");
        write(file, "version=1\nmax.records=10\n");
        try (IgniteConfig.HistorySettings settings = new IgniteConfig.HistorySettings(config(file))) {
            assertEquals(10, settings.current().maxRecords);
            write(file, "version=2\nmax.records=20\n"); settings.refresh();
            assertEquals(20, settings.current().maxRecords);
            for (String invalid : new String[]{"version=1\nmax.records=5", "version=2\nmax.records=5",
                    "version=3\nmax.records=0", "version=3", "", "version=3\nmax.records=1001"}) {
                write(file, invalid); settings.refresh();
                assertEquals(20, settings.current().maxRecords, invalid);
                assertEquals(2, settings.current().version);
                assertTrue(settings.lastRefreshFailed());
            }
            Files.delete(file); settings.refresh();
            assertEquals(20, settings.current().maxRecords);
            assertTrue(settings.lastRefreshFailed());
            write(file, "version=3\nmax.records=5\n"); settings.refresh();
            assertEquals(5, settings.current().maxRecords);
            assertFalse(settings.lastRefreshFailed());
        }
    }

    @ParameterizedTest @ValueSource(strings = {"", "version=0\nmax.records=10", "version=1\nmax.records=-1",
            "version=1\nmax.records=1.5", "version=1\nmax.records=1001", "version=x\nmax.records=10",
            "version=1\nmax.records=10\nunknown=x", "version=1\nversion=2\nmax.records=10"})
    void invalidInitialPolicyFailsInsteadOfSilentlyFallingBack(String contents) throws Exception {
        Path file = directory.resolve("history.properties"); write(file, contents);
        assertThrows(IllegalArgumentException.class, () -> new IgniteConfig.HistorySettings(config(file)));
    }

    @Test void oversizedAndMalformedUtf8FilesAreRejected() throws Exception {
        Path file = directory.resolve("history.properties");
        Files.write(file, new byte[8193]);
        assertThrows(IllegalArgumentException.class, () -> new IgniteConfig.HistorySettings(config(file)));
        Files.write(file, new byte[]{(byte) 0xff});
        assertThrows(IllegalArgumentException.class, () -> new IgniteConfig.HistorySettings(config(file)));
    }
}
