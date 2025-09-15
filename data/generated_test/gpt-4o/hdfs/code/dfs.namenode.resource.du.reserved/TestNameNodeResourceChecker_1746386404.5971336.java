package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.NameNodeResourceChecker;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestNameNodeResourceChecker {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testNameNodeResourceCheckerInitializationWithDefaultConfig() throws Exception {
        // Prepare the test conditions: Create a configuration object
        Configuration conf = new Configuration();

        // Set up a test directory using a writable temporary directory
        Path testDir = new Path(System.getProperty("test.build.data", "target/test/data") + "/dfs/name");
        FileSystem localFs = FileSystem.getLocal(conf);
        if (!localFs.exists(testDir)) {
            localFs.mkdirs(testDir);
        }

        // Configure the test directory in the configuration
        conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, testDir.toString());

        // Use the HDFS 2.8.5 API to retrieve the default configuration value of `dfs.namenode.resource.du.reserved`
        long expectedDuReserved = conf.getLong(
                DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_KEY,
                DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_DEFAULT
        );

        // Test code: Instantiate NameNodeResourceChecker with the configuration
        NameNodeResourceChecker nameNodeResourceChecker = new NameNodeResourceChecker(conf);

        // Access the private field `duReserved` using reflection for validation
        java.lang.reflect.Field field = NameNodeResourceChecker.class.getDeclaredField("duReserved");
        field.setAccessible(true);
        long actualDuReserved = field.getLong(nameNodeResourceChecker);

        // Assert that the duReserved field in NameNodeResourceChecker matches the default configuration value
        assertEquals("duReserved value should match the default configuration", expectedDuReserved, actualDuReserved);

        // Clean up: Remove the test directory to ensure no side effects
        if (localFs.exists(testDir)) {
            localFs.delete(testDir, true);
        }
    }

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to construct relevant objects.
    // 2. Prepare the test conditions for checking resource availability.
    // 3. Test code to invoke `hasAvailableDiskSpace` and validate its behavior.
    // 4. Code after testing.
    public void testHasAvailableDiskSpaceBehavior() throws Exception {
        // Prepare the test conditions: Create a configuration
        Configuration conf = new Configuration();

        // Set up a writable temporary directory with sufficient space
        Path testDir = new Path(System.getProperty("test.build.data", "target/test/data") + "/dfs/name");
        FileSystem localFs = FileSystem.getLocal(conf);
        if (!localFs.exists(testDir)) {
            localFs.mkdirs(testDir);
        }
        conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, testDir.toString());

        // Ensure the local file system disk usage threshold does not block the test
        localFs.setWorkingDirectory(testDir);

        // Instantiate NameNodeResourceChecker with the configuration
        NameNodeResourceChecker nameNodeResourceChecker = new NameNodeResourceChecker(conf);

        // Test the `hasAvailableDiskSpace` logic with sufficient available disk space
        boolean hasAvailableDiskSpace = nameNodeResourceChecker.hasAvailableDiskSpace();

        // Assert that the resource is marked available as per the default configuration threshold
        assertTrue("Resources should be available for the current disk space", hasAvailableDiskSpace);

        // Clean up: Remove the test directory
        if (localFs.exists(testDir)) {
            localFs.delete(testDir, true);
        }
    }
}