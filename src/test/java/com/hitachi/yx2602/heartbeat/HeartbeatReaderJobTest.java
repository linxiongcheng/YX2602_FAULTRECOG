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

class HeartbeatReaderJobTest {
    @TempDir Path directory;

    @ParameterizedTest
    @ValueSource(strings = {"--check-config", "--check-ignite", "--preview", "--run", "--watch-history-config", "--publish-history-config"})
    void retainsExistingModesWithoutNameOverride(String mode) {
        assertNull(HeartbeatReaderJob.requestedJobName(new String[]{"--config", "local.properties", mode}));
    }

    @ParameterizedTest @ValueSource(strings = {"--run", "--preview"})
    void acceptsOneLiteralNameForExecutionModes(String mode) {
        String name = "夜班 heartbeat $(literal) ${literal}";
        assertEquals(name, HeartbeatReaderJob.requestedJobName(
                new String[]{"--config", "local.properties", mode, "--job-name", name}));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "-bad", " padded", "padded ", "line\nbreak", "line\rbreak"})
    void rejectsInvalidNames(String name) {
        assertThrows(IllegalArgumentException.class, () -> HeartbeatReaderJob.requestedJobName(
                new String[]{"--config", "local.properties", "--run", "--job-name", name}));
    }

    @ParameterizedTest
    @ValueSource(strings = {"--check-config", "--check-ignite", "--watch-history-config", "--publish-history-config"})
    void nameOverrideIsNotSilentlyIgnoredByOtherModes(String mode) {
        assertThrows(IllegalArgumentException.class, () -> HeartbeatReaderJob.requestedJobName(
                new String[]{"--config", "local.properties", mode, "--job-name", "new-name"}));
    }

    @Test void rejectsMissingUnknownAndRepeatedOptionsBeforeReadingConfiguration() {
        for (String[] args : new String[][]{
                {}, {"--config"}, {"--config", "missing", "--run", "--job-name"},
                {"--config", "missing", "--run", "--unknown", "new-name"},
                {"--config", "missing", "--unknown"},
                {"--config", "missing", "--run", "--job-name", "first", "--job-name", "second"}}) {
            assertThrows(IllegalArgumentException.class, () -> HeartbeatReaderJob.main(args));
        }
    }

    @Test void overrideDoesNotRewritePropertiesAndLegacyConfigCheckStillWorks() throws Exception {
        Path file = directory.resolve("local.properties");
        Properties properties = ReaderConfigTest.properties();
        properties.setProperty("job.name", "configured-name");
        try (java.io.Writer writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) { properties.store(writer, null); }
        byte[] before = Files.readAllBytes(file);
        String requested = HeartbeatReaderJob.requestedJobName(
                new String[]{"--config", file.toString(), "--run", "--job-name", "temporary-name"});
        assertEquals("temporary-name", requested);
        assertEquals("configured-name", ReaderConfig.load(file).jobName);
        HeartbeatReaderJob.main(new String[]{"--config", file.toString(), "--check-config"});
        assertArrayEquals(before, Files.readAllBytes(file));
    }
}
