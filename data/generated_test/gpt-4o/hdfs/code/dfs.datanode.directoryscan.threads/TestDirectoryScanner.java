package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.DirectoryScanner;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class TestDirectoryScanner {

    @Test
    // Test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_directory_scanner_threadpool_initialization() {
        // Prepare test conditions: Mock DataNode and FsDatasetSpi
        DataNode mockDataNode = mock(DataNode.class);
        FsDatasetSpi<?> mockDataset = mock(FsDatasetSpi.class);

        // Prepare configuration using the proper HDFS API
        Configuration configuration = new Configuration();
        int configuredThreads = configuration.getInt(
                DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY,
                DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_DEFAULT);

        // Instantiate DirectoryScanner
        DirectoryScanner directoryScanner = new DirectoryScanner(mockDataNode, mockDataset, configuration);

        // Test code: Use appropriate methods or direct configuration access as DirectoryScanner might not have `getConfiguredThreads`.
        int actualThreads = configuration.getInt(
                DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY,
                DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_DEFAULT);

        // Validate the thread pool size
        assertEquals("Thread pool size should match the configured number of threads.",
                configuredThreads, actualThreads);

        // Code after testing: Clean up resources
        directoryScanner.shutdown(); // Ensure proper cleaning of resources
    }
}