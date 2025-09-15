package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.NameNodeResourceChecker;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

public class TestNameNodeResourceChecker {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_duReservedEdgeCase_zeroReservedSpace() throws IOException {
        // Step 1: Configure the reserved space to zero
        Configuration conf = new Configuration();
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_KEY, 0);

        // Step 2: Prepare the test directories
        File baseDir = new File(System.getProperty("java.io.tmpdir"), "testBaseDir");
        File tempDir = new File(baseDir, "testVolume");
        if (!tempDir.exists()) {
            assertTrue(tempDir.mkdirs());
        }

        Path volumePath = new Path(tempDir.getAbsolutePath());

        // Step 3: Initialize a local file system to simulate available space
        LocalFileSystem localFs = FileSystem.getLocal(conf);
        assertTrue(localFs instanceof LocalFileSystem);

        // Step 4: Add the directory into the NameNode configuration
        conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, baseDir.getAbsolutePath());

        // Step 5: Create a NameNodeResourceChecker instance
        NameNodeResourceChecker checker = new NameNodeResourceChecker(conf);

        // Step 6: Validate isResourceAvailable - should return true due to zero reserved space
        boolean isAvailable = checker.hasAvailableDiskSpace();
        assertTrue("hasAvailableDiskSpace() should return true when reserved space is set to zero.", isAvailable);

        // Step 7: Clean up the test directory
        assertTrue(tempDir.delete());
        assertTrue(baseDir.delete());
    }
}