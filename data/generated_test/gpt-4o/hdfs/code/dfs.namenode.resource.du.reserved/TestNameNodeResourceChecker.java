package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.NameNodeResourceChecker;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class TestNameNodeResourceChecker {

    @Test
    // test case: test_isResourceAvailable_belowReservedSpace
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_isResourceAvailable_belowReservedSpace() throws IOException {
        // Step 1: Retrieve configuration values using the HDFS 2.8.5 API.
        Configuration conf = new Configuration();
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_KEY, 200 * 1024 * 1024); // Set reserved space to 200 MB
        long reservedSpace = conf.getLong(DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_KEY, DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_DEFAULT);

        // Step 2: Create a test directory structure mimicking the required NameNode directory.
        File testDirectory = new File(System.getProperty("java.io.tmpdir"), "hadoop/dfs/name");
        if (!testDirectory.exists() && !testDirectory.mkdirs()) {
            throw new IOException("Failed to create the test directory structure.");
        }

        // Step 3: Configure the NameNodeResourceChecker.
        conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, testDirectory.getAbsolutePath());
        NameNodeResourceChecker resourceChecker = new NameNodeResourceChecker(conf);

        // Directly check usable space within the test directory.
        long usableSpace = testDirectory.getUsableSpace(); // Get usable space.

        // Test if the resourceChecker recognizes the available space correctly.
        Assert.assertTrue("Disk space is not correctly recognized as available.", usableSpace >= reservedSpace);

        // Step 4: Cleanup after testing.
        File[] files = testDirectory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (!file.delete()) {
                    throw new IOException("Failed to delete test file: " + file.getAbsolutePath());
                }
            }
        }
        if (!testDirectory.delete()) {
            throw new IOException("Failed to delete the test directory structure.");
        }
    }
}