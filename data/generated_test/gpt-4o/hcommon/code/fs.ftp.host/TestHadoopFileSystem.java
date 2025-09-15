package org.apache.hadoop.test;

import org.apache.hadoop.conf.Configuration;    
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.OutputStream;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class TestHadoopFileSystem {
    private FileSystem fs;
    private Configuration conf;
    private Path testFilePath;

    @Before
    public void setUp() throws Exception {
        // 1. Use API to get configuration values, avoid hardcoding
        conf = new Configuration();
        // Set the test directory to a temporary folder in the local filesystem to avoid permission issues
        conf.set("fs.defaultFS", "file:///");
        Path basePath = new Path(System.getProperty("java.io.tmpdir")); 
        testFilePath = new Path(basePath, "test-file");

        fs = FileSystem.get(conf);
    }

    @Test
    public void testFileSystemCreateAndDelete() throws Exception {
        // 2. Prepare test conditions
        if (fs.exists(testFilePath)) {
            fs.delete(testFilePath, false);
        }

        // 3. Test code
        // Create the file
        try (OutputStream out = fs.create(testFilePath)) {
            out.write("testing write".getBytes());
        }

        // Assert the file was created
        assertTrue("File should exist after creation", fs.exists(testFilePath));

        // Delete the file
        fs.delete(testFilePath, false);

        // Assert the file was deleted
        assertFalse("File should not exist after deletion", fs.exists(testFilePath));
    }

    @After
    public void tearDown() throws Exception {
        // 4. Cleanup after testing
        if (fs.exists(testFilePath)) {
            fs.delete(testFilePath, false);
        }
        fs.close();
    }
}