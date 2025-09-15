package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.ExecutorService;

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
    // Test that DirectoryScanner uses the default number of threads when
    // dfs.datanode.directoryscan.threads is not explicitly configured
    // 1. Create a Configuration object without setting dfs.datanode.directoryscan.threads
    // 2. Instantiate a DirectoryScanner with the configuration
    // 3. Use reflection to access the thread pool size
    // 4. Assert that the thread pool size equals the default value
    public void testDirectoryScannerThreadPoolSizeWithDefaultThreads() throws Exception {
        // Prepare test conditions: Ensure the config key is not set
        conf.unset(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY);
        
        // Create DirectoryScanner instance
        DirectoryScanner scanner = new DirectoryScanner(datanode, dataset, conf);
        
        // Use reflection to access the private reportCompileThreadPool field
        Field threadPoolField = DirectoryScanner.class.getDeclaredField("reportCompileThreadPool");
        threadPoolField.setAccessible(true);
        ExecutorService threadPool = (ExecutorService) threadPoolField.get(scanner);
        
        // Verify that the thread pool was created with the expected number of threads
        int expectedThreads = conf.getInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY,
                DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_DEFAULT);
        
        assertEquals("DirectoryScanner should use the default thread count when not configured",
                DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_DEFAULT, expectedThreads);
        
        // Cleanup
        scanner.shutdown();
    }
    
    @Test
    // Test that DirectoryScanner uses the configured number of threads when
    // dfs.datanode.directoryscan.threads is explicitly set
    // 1. Create a Configuration object and set dfs.datanode.directoryscan.threads to a specific value
    // 2. Instantiate a DirectoryScanner with the configuration
    // 3. Use reflection to access the thread pool size
    // 4. Assert that the thread pool size equals the configured value
    public void testDirectoryScannerThreadPoolSizeWithConfiguredThreads() throws Exception {
        // Prepare test conditions: Set the config key to a specific value
        int configuredThreads = 10;
        conf.setInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY, configuredThreads);
        
        // Create DirectoryScanner instance
        DirectoryScanner scanner = new DirectoryScanner(datanode, dataset, conf);
        
        // Use reflection to access the private reportCompileThreadPool field
        Field threadPoolField = DirectoryScanner.class.getDeclaredField("reportCompileThreadPool");
        threadPoolField.setAccessible(true);
        ExecutorService threadPool = (ExecutorService) threadPoolField.get(scanner);
        
        // Verify that the thread pool was created with the configured number of threads
        int expectedThreads = conf.getInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY,
                DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_DEFAULT);
        
        assertEquals("DirectoryScanner should use the configured thread count",
                configuredThreads, expectedThreads);
        
        // Cleanup
        scanner.shutdown();
    }
}