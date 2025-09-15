package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import static org.junit.Assert.*;

public class HDFSConfigurationTest {

    private Configuration conf;
    private FileSystem fs;

    @Before
    // Prepare the test conditions.
    public void setUp() throws Exception {
        conf = new Configuration();
        conf.set("fs.defaultFS", "file:///"); // Use local filesystem
        
        // Adding a dummy Hadoop directory setup for the test environment
        System.setProperty("hadoop.home.dir", "/tmp/hadoop-fake-home");

        // Set permissions for the local test directory
        Path baseTestDir = new Path("/tmp/hadoop-test-dir");
        FileSystem localFs = FileSystem.get(conf);
        if (!localFs.exists(baseTestDir)) {
            localFs.mkdirs(baseTestDir);
        }
        
        System.setProperty("test.hdfs.base.dir", baseTestDir.toString());
        fs = FileSystem.get(conf); // Create FileSystem instance
    }

    @Test
    // Test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Test specific behavior or functionality in the configuration and HDFS client.
    public void testConfigurationValue() throws Exception {
        String defaultFS = conf.get("fs.defaultFS"); // Correctly use HDFS API for retrieval
        assertNotNull("defaultFS should not be null", defaultFS);
        assertEquals("fs.defaultFS value mismatch", "file:///", defaultFS);

        // Test if the filesystem is accessible and operational
        Path baseDir = new Path(System.getProperty("test.hdfs.base.dir"));
        Path testPath = new Path(baseDir, "testPath");
        
        if (fs.exists(testPath)) {
            fs.delete(testPath, true); // Clean up before test
        }
        fs.create(testPath).close(); // Create test path
        assertTrue("testPath should exist after creation", fs.exists(testPath));
    }

    @After
    // Code after testing.
    public void tearDown() throws Exception {
        if (fs != null) {
            Path baseDir = new Path(System.getProperty("test.hdfs.base.dir"));
            Path testPath = new Path(baseDir, "testPath");
            if (fs.exists(testPath)) {
                fs.delete(testPath, true); // Clean up after test
            }
            fs.close(); // Close FileSystem instance
        }
    }
}