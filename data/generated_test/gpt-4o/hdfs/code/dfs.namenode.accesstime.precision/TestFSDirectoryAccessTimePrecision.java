package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestFSDirectoryAccessTimePrecision {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testGetAccessTimePrecisionConfiguration() {
        // Step 1: Initialize a configuration object using the HDFS API.
        Configuration conf = new HdfsConfiguration();

        // Step 2: Define configuration keys and set test-specific values.
        final String DFS_NAMENODE_ACCESSTIME_PRECISION_KEY = "dfs.namenode.accesstime.precision";
        final long DFS_NAMENODE_ACCESSTIME_PRECISION_DEFAULT = 3600000L; // default value in milliseconds
        conf.setLong(DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, DFS_NAMENODE_ACCESSTIME_PRECISION_DEFAULT);

        // Step 3: Retrieve the configured value to validate correctness of the test setup.
        long expectedAccessTimePrecision = conf.getLong(
                DFS_NAMENODE_ACCESSTIME_PRECISION_KEY,
                DFS_NAMENODE_ACCESSTIME_PRECISION_DEFAULT);

        // Step 4: Prepare the FSImage and FSNamesystem with the correct instantiation.
        FSImage fsImage = null;
        FSNamesystem fsNamesystem = null;

        try {
            fsImage = new FSImage(conf);
            fsNamesystem = new FSNamesystem(conf, fsImage); // Correct instantiation of FSNamesystem
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize FSNamesystem: " + e.getMessage(), e);
        }

        // Create an FSDirectory instance using FSNamesystem.
        FSDirectory fsDirectory = fsNamesystem.getFSDirectory();

        // Step 5: Perform the test by calling getAccessTimePrecision().
        long actualAccessTimePrecision = fsDirectory.getAccessTimePrecision();

        // Step 6: Validate the result: the returned value should match the value in the configuration.
        assertEquals("Access time precision value does not match the configured value.",
                expectedAccessTimePrecision, actualAccessTimePrecision);

        // No cleanup is necessary as this is a configuration-only test.
    }
}