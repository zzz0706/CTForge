package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DirectoryScannerConfigTest {

    private Configuration conf;

    @Before
    public void setUp() {
        conf = new Configuration();
    }

    @Test
    public void testDfsDatanodeDirectoryscanThreadsDefaultValue() {
        // Given: No explicit configuration set for dfs.datanode.directoryscan.threads
        // When: Getting the configuration value
        int threadCount = conf.getInt(
                DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY,
                DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_DEFAULT
        );
        
        // Then: The default value should be used
        assertEquals("Default thread count should be 1",
                DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_DEFAULT,
                threadCount);
    }

    @Test
    public void testDfsDatanodeDirectoryscanThreadsCustomValue() {
        // Given: A custom configuration value for dfs.datanode.directoryscan.threads
        int customThreadCount = 5;
        conf.setInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY, customThreadCount);

        // When: Getting the configuration value
        int threadCount = conf.getInt(
                DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY,
                DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_DEFAULT
        );
        
        // Then: The custom value should be used
        assertEquals("Custom thread count should match configuration",
                customThreadCount,
                threadCount);
    }

    @Test
    public void testDfsDatanodeDirectoryscanThreadsFromFile() {
        // Given: Configuration loaded from file (simulated here by setting via API)
        // In a real test, you would load from an actual config file
        conf.addResource("hdfs-site.xml"); // This would be your test resource file
        
        // When: Getting the configuration value
        int configuredThreads = conf.getInt(
                DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY,
                DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_DEFAULT
        );

        // Then: Compare against the default if not explicitly set in the file
        // For this test, we assume no override in the file, so it should be default
        assertEquals("Configuration value should match default when not overridden in file",
                DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_DEFAULT,
                configuredThreads);
    }
}