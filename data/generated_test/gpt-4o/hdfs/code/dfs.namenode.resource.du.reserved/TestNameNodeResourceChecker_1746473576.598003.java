package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertTrue;

public class TestNameNodeResourceChecker {

    private File testDirectory;
    private Configuration conf;
    private LocalFileSystem localFs;

    @Before
    public void setUp() throws Exception {
        // Prepare the configuration
        conf = new Configuration();
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_KEY, DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_DEFAULT);

        // Use the correct method to initialize LocalFileSystem
        FileSystem fs = FileSystem.getLocal(conf);
        if (fs instanceof LocalFileSystem) {
            localFs = (LocalFileSystem) fs;
        } else {
            throw new IllegalStateException("Expected a LocalFileSystem instance");
        }

        // Retrieve the configured reserved space value using HDFS API
        long reservedSpace = conf.getLong(DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_KEY, DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_DEFAULT);

        // Create a temporary directory mimicking a disk volume
        testDirectory = new File("test-volume");
        if (!testDirectory.exists()) {
            assertTrue("Failed to create test directory", testDirectory.mkdir());
        }

        // Simulate a volume using LocalFileSystem
        Path volumePath = localFs.makeQualified(new Path(testDirectory.getAbsolutePath()));

        // Ensure test conditions match prerequisites
        assertTrue("Test directory should exist", testDirectory.exists());
        assertTrue("Path should exist on LocalFileSystem", localFs.exists(volumePath));
    }

    @After
    public void tearDown() throws Exception {
        // Clean up temporary test directory
        if (testDirectory.exists()) {
            assertTrue("Failed to delete test directory", testDirectory.delete());
        }
    }

    @Test
    public void testReservedSpaceConfiguration() throws Exception {
        // 1. Use the HDFS 2.8.5 API correctly to obtain configuration values.
        long retrievedReservedSpace = conf.getLong(DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_KEY, DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_DEFAULT);

        // 2. Prepare the test conditions.
        assertTrue("Test directory should exist", testDirectory.exists());

        // 3. Test code.
        assertTrue("Retrieved reserved space should match default value", retrievedReservedSpace == DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_DEFAULT);

        // 4. Code after testing to clean up remains handled in tearDown().
    }
}