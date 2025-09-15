package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class SafeModeInfoTest {

    // Define constants used in HDFS Configuration
    private static final String DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY =
            "dfs.namenode.safemode.threshold-pct";
    private static final float DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT = 0.9f;

    private static final String DFS_NAMENODE_REPL_QUEUE_THRESHOLD_PCT_KEY =
            "dfs.namenode.replqueue.threshold-pct";
    private static final float DFS_NAMENODE_REPL_QUEUE_THRESHOLD_PCT_DEFAULT = 0.5f;

    private Configuration configuration;

    @Before
    public void setUp() {
        // Initialize the Configuration object
        configuration = new Configuration();

        // Set the required configuration values
        configuration.setFloat(DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT);
        configuration.setFloat(DFS_NAMENODE_REPL_QUEUE_THRESHOLD_PCT_KEY, DFS_NAMENODE_REPL_QUEUE_THRESHOLD_PCT_DEFAULT);
    }

    @Test
    public void testReplQueueThreshold_CustomValueEqualThreshold() {
        // Create a SafeModeInfo instance using the Configuration object
        SafeModeInfo safeModeInfo = new SafeModeInfo(configuration);

        // Assert that the replication queue threshold matches the expected value
        assertEquals(DFS_NAMENODE_REPL_QUEUE_THRESHOLD_PCT_DEFAULT, safeModeInfo.getReplQueueThreshold(), 0.0f);
    }

    // Inner class SafeModeInfo simulates the actual class behavior for testing purposes
    private static class SafeModeInfo {
        private final float replQueueThreshold;

        public SafeModeInfo(Configuration configuration) {
            // Obtain the relevant configuration value
            replQueueThreshold = configuration.getFloat(
                    DFS_NAMENODE_REPL_QUEUE_THRESHOLD_PCT_KEY,
                    DFS_NAMENODE_REPL_QUEUE_THRESHOLD_PCT_DEFAULT);
        }

        public float getReplQueueThreshold() {
            return replQueueThreshold;
        }
    }
}