package com.hitachi.yx2602.heartbeat;

import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class PackagedIgniteIT {
    @TempDir Path directory;

    @Test @Timeout(90) void shadedArtifactConnectsWritesAndReadsActualIgnite() throws Exception {
        IgniteStoreTest.directory = directory;
        try {
            IgniteStoreTest.provision();
            String java = Paths.get(System.getProperty("java.home"), "bin", "java").toString();
            String classpath = Paths.get("target/heartbeat-reader-0.1.0.jar").toAbsolutePath()
                    + File.pathSeparator + Paths.get("target/test-classes").toAbsolutePath();
            Path log = Paths.get("target/packaged-ignite-smoke.log");
            Process process = new ProcessBuilder(java,
                    "--add-opens=java.base/java.nio=ALL-UNNAMED", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                    "--add-opens=java.base/java.lang=ALL-UNNAMED", "--add-opens=java.base/java.util=ALL-UNNAMED",
                    "--add-opens=java.base/java.io=ALL-UNNAMED", "-cp", classpath,
                    PackagedIgniteClient.class.getName(), "127.0.0.1:10990")
                    .redirectErrorStream(true).redirectOutput(log.toFile()).start();
            try {
                assertTrue(process.waitFor(45, TimeUnit.SECONDS), "packaged client timed out");
                String output = new String(Files.readAllBytes(log), StandardCharsets.UTF_8);
                assertEquals(0, process.exitValue(), output);
                assertTrue(output.contains("PACKAGED_IGNITE_SMOKE_OK"), output);
            } finally { if (process.isAlive()) process.destroyForcibly(); }
        } finally { IgniteStoreTest.stopServer(); }
    }

    @Test void deploymentXmlLoadsWithPersistenceAndFsyncEnabled() throws Exception {
        IgniteConfiguration config = Ignition.loadSpringBean(
                Paths.get("config/ignite2-single-node.xml").toUri().toURL(), "ignite.cfg");
        assertEquals("yx2602-heartbeat-node-1", config.getConsistentId());
        assertTrue(config.getDataStorageConfiguration().getDefaultDataRegionConfiguration().isPersistenceEnabled());
        assertEquals("heartbeat", config.getDataStorageConfiguration().getDefaultDataRegionConfiguration().getName());
        assertEquals(WALMode.FSYNC, config.getDataStorageConfiguration().getWalMode());
        assertEquals(10800, config.getClientConnectorConfiguration().getPort());
    }
}
