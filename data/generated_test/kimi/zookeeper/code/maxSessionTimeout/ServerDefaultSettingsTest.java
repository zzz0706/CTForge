package org.apache.zookeeper.test;

import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Validates the default configuration settings of a ZooKeeperServer instance,
 * specifically focusing on session timeout policies.
 */
public class ServerDefaultSettingsTest {

    // Define constants for configuration to avoid magic numbers.
    private static final int BASE_TICK_TIME = 2000;
    private static final int UNSPECIFIED_MAX_SESSION_TIMEOUT = -1;
    private static final int DEFAULT_MAX_TIMEOUT_MULTIPLIER = 20;

    private ZooKeeperServer serverInstance;
    private File transactionLogDir;
    private File snapshotDir;

    /**
     * Sets up the test environment before each test case runs. This includes
     * creating temporary directories and initializing the ZooKeeperServer.
     */
    @Before
    public void setupTestEnvironment() throws IOException {
        // Create temporary directories for logs and snapshots.
        transactionLogDir = createTemporaryDirectory("zookeeper_logs");
        snapshotDir = createTemporaryDirectory("zookeeper_snaps");

        FileTxnSnapLog logAndSnapshotManager = new FileTxnSnapLog(transactionLogDir, snapshotDir);

        // Define session timeout parameters based on the base tick time.
        int minSessionTimeout = BASE_TICK_TIME * 2;

        // Instantiate the ZooKeeperServer with the default max session timeout setting.
        serverInstance = new ZooKeeperServer(
            logAndSnapshotManager,
            BASE_TICK_TIME,
            minSessionTimeout,
            UNSPECIFIED_MAX_SESSION_TIMEOUT, // Use the "unspecified" flag.
            null
        );
    }

    /**
     * Cleans up the test environment after each test case, ensuring that
     * temporary files and directories are removed.
     */
    @After
    public void cleanupTestEnvironment() {
        // Recursively delete temporary directories and their contents.
        deleteDirectory(transactionLogDir);
        deleteDirectory(snapshotDir);
    }

    /**
     * Verifies that when the max session timeout is not explicitly configured,
     * the ZooKeeperServer correctly calculates a default value, which is
     * defined as 20 times the 'tickTime'.
     */
    @Test
    public void verifyMaxSessionTimeoutDefaultsCorrectlyWhenUnspecified() {
        // Retrieve the actual max session timeout value from the server instance.
        int actualMaxTimeout = serverInstance.getMaxSessionTimeout();

        // Calculate the expected default value.
        int expectedDefaultMaxTimeout = BASE_TICK_TIME * DEFAULT_MAX_TIMEOUT_MULTIPLIER;

        // Assert that the retrieved value matches the expected default.
        assertEquals(
            "The default max session timeout should be 20 times the tickTime.",
            expectedDefaultMaxTimeout,
            actualMaxTimeout
        );
    }

    /**
     * Helper method to create a temporary directory for test artifacts.
     */
    private File createTemporaryDirectory(String prefix) throws IOException {
        File tempFile = File.createTempFile(prefix, "");
        assertTrue(tempFile.delete()); // Delete the file to replace it with a directory.
        assertTrue(tempFile.mkdir());  // Create the directory.
        return tempFile;
    }

    /**
     * Helper method to recursively delete a directory.
     */
    private void deleteDirectory(File directory) {
        if (directory != null && directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    deleteDirectory(file);
                }
            }
            directory.delete();
        }
    }
}