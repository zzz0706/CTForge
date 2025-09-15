package org.apache.zookeeper.test;

import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class ZooKeeperServerTest {

    @Test
    // Test code for verifying `getDataDirSize` functionality
    // 1. You need to use the zookeeper3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getDataDirSize_validConfiguration() {
        File tempDataDir = null;
        try {
            // 1. Use the Zookeeper3.5.6 API to create test environment
            tempDataDir = new File(System.getProperty("java.io.tmpdir"), "testDataDir");
            if (!tempDataDir.exists()) {
                tempDataDir.mkdir();
            }
            
            // Create sample snapshot and log files in the data directory
            File sampleSnapshot = new File(tempDataDir, "snapshot1");
            File sampleLog = new File(tempDataDir, "log1");
            sampleSnapshot.createNewFile();
            sampleLog.createNewFile();

            // Write some dummy data to the files to simulate snapshots/logs
            try (java.io.FileWriter writer = new java.io.FileWriter(sampleSnapshot)) {
                writer.write("Sample snapshot data");
            }
            try (java.io.FileWriter writer = new java.io.FileWriter(sampleLog)) {
                writer.write("Sample log data");
            }

            // Prepare the ZooKeeperServer with proper configuration
            FileTxnSnapLog fileTxnSnapLog = new FileTxnSnapLog(tempDataDir, tempDataDir);
            ZooKeeperServer zkServer = new ZooKeeperServer();
            zkServer.setTxnLogFactory(fileTxnSnapLog);

            // 2. Test logic to compute the size of the data directory
            long dataDirSize = zkServer.getTxnLogFactory().getDataDir().length() + zkServer.getTxnLogFactory().getSnapDir().length();
            
            // Validate the computed size vs expected size
            long expectedSize = sampleSnapshot.length() + sampleLog.length();
            Assert.assertEquals("Expected size should match actual size of files", expectedSize, dataDirSize);
        } catch (Exception e) {
            Assert.fail("Test failed due to unexpected exception: " + e.getMessage());
        } finally {
            // 3. Cleanup after test
            if (tempDataDir != null && tempDataDir.exists()) {
                for (File file : tempDataDir.listFiles()) {
                    file.delete();
                }
                tempDataDir.delete();
            }
        }
    }
}