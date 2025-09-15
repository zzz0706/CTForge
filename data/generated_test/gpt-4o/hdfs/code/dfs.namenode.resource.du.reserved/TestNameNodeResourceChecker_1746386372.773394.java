package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.NameNodeResourceChecker;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestNameNodeResourceChecker {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testNameNodeResourceCheckerInitializationWithDefaultConfig() throws Exception {
        // Prepare the test conditions: Create a mock Configuration object
        Configuration conf = new Configuration();

        // Set up the required test directory using a writable temp directory
        Path testDir = new Path(System.getProperty("test.build.data", "target/test/data") + "/dfs/name");
        FileSystem localFs = FileSystem.getLocal(conf);
        if (!localFs.exists(testDir)) {
            localFs.mkdirs(testDir);
        }

        // Configure the test directory in the configuration
        conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, testDir.toString());

        // Use the HDFS 2.8.5 API to retrieve default configuration values
        long expectedDuReserved = conf.getLong(
                DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_KEY,
                DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_DEFAULT
        );

        // Test code: Instantiate NameNodeResourceChecker with the configuration
        NameNodeResourceChecker nameNodeResourceChecker = new NameNodeResourceChecker(conf);

        // Access the private field `duReserved` using reflection
        java.lang.reflect.Field field = NameNodeResourceChecker.class.getDeclaredField("duReserved");
        field.setAccessible(true);  // Make the field accessible
        long actualDuReserved = field.getLong(nameNodeResourceChecker);

        // Verify that the duReserved field in NameNodeResourceChecker matches the default configuration value
        assertEquals("NameNodeResourceChecker should use the default duReserved value",
                expectedDuReserved, actualDuReserved);

        // Clean up: Remove the test directory after testing
        if (localFs.exists(testDir)) {
            localFs.delete(testDir, true);
        }
    }
}