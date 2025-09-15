package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi.FsVolumeReferences;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class DirectoryScannerConfigTest {

    @Mock
    private DataNode datanode;
    
    @Mock
    private FsDatasetSpi<?> dataset;
    
    @Mock
    private FsVolumeReferences volumeReferences;
    
    @Mock
    private FsVolumeSpi volume1, volume2, volume3;

    private Configuration conf;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        conf = new Configuration();
        
        // Mock volume references with 3 volumes
        when(dataset.getFsVolumeReferences()).thenReturn(volumeReferences);
        when(volumeReferences.size()).thenReturn(3);
        when(volumeReferences.get(0)).thenReturn(volume1);
        when(volumeReferences.get(1)).thenReturn(volume2);
        when(volumeReferences.get(2)).thenReturn(volume3);
        when(volumeReferences.iterator()).thenReturn(
            java.util.Arrays.asList(volume1, volume2, volume3).iterator()
        );
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
    
    @Test
    public void testDirectoryScannerThreadPoolCreationWithMultipleThreads() throws Exception {
        // Given: A Hadoop configuration object with dfs.datanode.directoryscan.threads set to a value greater than 1
        int threadCount = 3;
        conf.setInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY, threadCount);
        
        // Mock a dataset with multiple volumes (3 volumes)
        when(dataset.getFsVolumeReferences()).thenReturn(volumeReferences);
        when(volumeReferences.size()).thenReturn(3);
        when(volumeReferences.get(0)).thenReturn(volume1);
        when(volumeReferences.get(1)).thenReturn(volume2);
        when(volumeReferences.get(2)).thenReturn(volume3);
        
        // Mock volume storage IDs
        when(volume1.getStorageID()).thenReturn("volume-1");
        when(volume2.getStorageID()).thenReturn("volume-2");
        when(volume3.getStorageID()).thenReturn("volume-3");

        // Create DirectoryScanner with correct constructor parameters
        // Note: We cannot directly test private methods, so we test the configuration effect indirectly
        DirectoryScanner scanner = new DirectoryScanner(datanode, dataset, conf);
        
        // Access the thread pool through reflection since it's private
        java.lang.reflect.Field threadPoolField = DirectoryScanner.class.getDeclaredField("reportCompileThreadPool");
        threadPoolField.setAccessible(true);
        ThreadPoolExecutor reportCompileThreadPool = (ThreadPoolExecutor) threadPoolField.get(scanner);
        
        // Verify that the thread pool was created with the correct size
        assertEquals("Thread pool size should match configured thread count", 
                threadCount, reportCompileThreadPool.getCorePoolSize());
        
        // Clean up
        scanner.shutdown();
    }
}