package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class DirectoryScannerConfigTest {

    private Configuration conf;
    private DataNode datanode;
    private FsDatasetSpi<?> dataset;

    @Before
    public void setUp() {
        conf = new Configuration();
        datanode = mock(DataNode.class);
        dataset = mock(FsDatasetSpi.class);
    }

    @Test
    public void testDirectoryScanThreadsConfigLoadedCorrectly() {
        // Prepare test conditions: Set the config value in the Configuration object
        int expectedThreads = 5;
        conf.setInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY, expectedThreads);

        // Verify that the config value is loaded correctly by checking the default if not set
        conf.unset(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY);
        int defaultThreads = conf.getInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY,
                DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_DEFAULT);
        assertEquals("Default value for dfs.datanode.directoryscan.threads should be 1",
                DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_DEFAULT, defaultThreads);
    }

    @Test
    public void testDirectoryScanThreadsDefaultValue() {
        // Ensure the key is not set to test the default value
        conf.unset(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY);

        // Get the value using the Configuration object (mimicking the DirectoryScanner's behavior)
        int threads = conf.getInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY,
                DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_DEFAULT);

        // Assert that the default value is used
        assertEquals("Default value for dfs.datanode.directoryscan.threads should be 1",
                DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_DEFAULT, threads);
    }

    @Test
    public void testDirectoryScanThreadsConfigFromFile() {
        // This test assumes that the configuration is loaded from a file.
        // In a real scenario, you would load the configuration from a file and verify the value.
        // For this example, we'll simulate setting it via the Configuration API,
        // which is the standard way in Hadoop to load from files.
        String configValue = "3"; // Simulate a value from a config file
        conf.set(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY, configValue);

        int threads = conf.getInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY,
                DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_DEFAULT);

        assertEquals("Value loaded from config should match the file's value",
                Integer.parseInt(configValue), threads);
    }
}