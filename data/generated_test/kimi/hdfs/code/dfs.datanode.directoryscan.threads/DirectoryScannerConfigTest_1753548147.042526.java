package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class DirectoryScannerConfigTest {

    private Configuration conf;
    @Mock
    private DataNode datanode;
    @Mock
    private FsDatasetSpi<?> dataset;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        conf = new Configuration();
    }

    @Test
    // testDirectoryScannerThreadPoolSizeWithCustomThreads
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions by setting dfs.datanode.directoryscan.threads to a custom value.
    // 3. Test code by instantiating DirectoryScanner with the configuration.
    // 4. Code after testing verifies that the thread pool size matches the configured value.
    public void testDirectoryScannerThreadPoolSizeWithCustomThreads() {
        // Set a custom value for the configuration
        int customThreads = 5;
        conf.setInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY, customThreads);

        // Instantiate a DirectoryScanner with the configuration
        DirectoryScanner scanner = new DirectoryScanner(datanode, dataset, conf);

        // Use reflection to access the private thread pool field
        try {
            Field field = DirectoryScanner.class.getDeclaredField("reportCompileThreadPool");
            field.setAccessible(true);
            ExecutorService executorService = (ExecutorService) field.get(scanner);

            // Assert that the captured thread pool size equals the configured value
            assertEquals("The thread pool size should match the configured value",
                    customThreads, ((ThreadPoolExecutor) executorService).getCorePoolSize());
        } catch (Exception e) {
            throw new RuntimeException("Failed to access or verify thread pool size", e);
        } finally {
            // Clean up resources
            scanner.shutdown();
        }
    }

    @Test
    // testDirectoryScannerThreadPoolSizeWithDefaultThreads
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions by ensuring no explicit configuration is set.
    // 3. Test code by instantiating DirectoryScanner with the default configuration.
    // 4. Code after testing verifies that the thread pool size matches the default value.
    public void testDirectoryScannerThreadPoolSizeWithDefaultThreads() {
        // Ensure the configuration is not explicitly set to test the default
        DirectoryScanner scanner = new DirectoryScanner(datanode, dataset, conf);

        // Use reflection to access the private thread pool field
        try {
            Field field = DirectoryScanner.class.getDeclaredField("reportCompileThreadPool");
            field.setAccessible(true);
            ExecutorService executorService = (ExecutorService) field.get(scanner);

            // Verify that the thread pool was created with the default value
            int expectedThreads = DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_DEFAULT;
            assertEquals("The thread pool size should match the default value",
                    expectedThreads, ((ThreadPoolExecutor) executorService).getCorePoolSize());
        } catch (Exception e) {
            throw new RuntimeException("Failed to access or verify thread pool size", e);
        } finally {
            // Clean up resources
            scanner.shutdown();
        }
    }
}