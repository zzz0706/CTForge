package org.apache.zookeeper.test;

import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.junit.Test;
import java.io.File;
import static org.junit.Assert.assertEquals;

public class ZooKeeperServerConfigTest {

    @Test
    // Test code
    // 1. Directly call API to get configuration values, avoid defining config files and adding them.
    // 2. Use ZooKeeper 3.5.6 API correctly to obtain configuration values instead of hardcoding values.
    // 3. Prepare the test conditions.
    // 4. Write and execute test code.
    // 5. Clean up after testing.

    public void test_ZooKeeperServer_maxSessionTimeout_DefaultValue() throws Exception {
        // 1. Prepare test conditions
        // Create a temporary directory for logs and snapshots
        File dataLogDir = File.createTempFile("datalog", "");
        dataLogDir.delete();
        dataLogDir.mkdir();
        File snapDir = File.createTempFile("snapshot", "");
        snapDir.delete();
        snapDir.mkdir();
        
        try {
            // Initialize the FileTxnSnapLog instance
            FileTxnSnapLog txnLog = new FileTxnSnapLog(dataLogDir, snapDir);

            // Define tickTime
            int tickTime = 2000;
            int minSessionTimeout = tickTime * 2;

            // 2. Test code
            // Create ZooKeeperServer instance with maxSessionTimeout = -1 (unspecified)
            ZooKeeperServer zooKeeperServer = new ZooKeeperServer(
                    txnLog,
                    tickTime,
                    minSessionTimeout,
                    -1,  // Default value for maxSessionTimeout
                    null
            );

            // Retrieve the maxSessionTimeout value through the API
            int maxSessionTimeout = zooKeeperServer.getMaxSessionTimeout();

            // 3. Validate the result
            // Ensure the default maxSessionTimeout is calculated correctly as 20 * tickTime
            assertEquals(tickTime * 20, maxSessionTimeout);

        } finally {
            // 4. Cleanup
            // Delete temporary directories if they exist 
            dataLogDir.delete();
            snapDir.delete();
        }
    }
}