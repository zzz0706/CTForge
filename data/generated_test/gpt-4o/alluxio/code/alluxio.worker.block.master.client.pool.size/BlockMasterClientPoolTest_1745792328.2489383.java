package alluxio.worker.block;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.resource.ResourcePool;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class BlockMasterClientPoolTest {

    @Test
    // test code
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_BlockMasterClientPool_acquire_with_custom_timeout() throws Exception {
        // Step 1: Use the Alluxio 2.1.0 API to correctly obtain configuration values
        // Get and set the configuration property `alluxio.worker.block.master.client.pool.size`
        int configuredPoolSize = ServerConfiguration.getInt(PropertyKey.WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE);
        ServerConfiguration.set(PropertyKey.WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE, Integer.toString(configuredPoolSize));

        // Step 2: Prepare the test conditions
        // Instantiate BlockMasterClientPool, which initializes according to the configuration
        BlockMasterClientPool clientPool = new BlockMasterClientPool();

        // Number of threads simulating concurrent resource usage
        int additionalThreads = 3; // Simulate a workload exceeding pool capacity
        int totalThreads = configuredPoolSize + additionalThreads;
        Thread[] threads = new Thread[totalThreads];
        boolean[] results = new boolean[totalThreads];

        for (int i = 0; i < totalThreads; i++) {
            final int threadIndex = i;
            threads[threadIndex] = new Thread(() -> {
                try {
                    // Step 3: Test code
                    // Acquire a resource with a timeout of 100ms
                    Object resource = clientPool.acquire(100, TimeUnit.MILLISECONDS);
                    results[threadIndex] = (resource != null); // If resource acquired, set true
                } catch (Exception e) {
                    results[threadIndex] = false; // Handle failures during acquisition
                }
            });
        }

        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }

        // Wait for all threads to finish
        for (Thread thread : threads) {
            thread.join();
        }

        // Step 4: Code after testing
        // Verify that successful allocations do not exceed configured pool size
        int successfulAcquisitions = 0;
        for (boolean result : results) {
            if (result) {
                successfulAcquisitions++;
            }
        }

        // Assert that the number of successful acquisitions matches the configured pool size
        Assert.assertEquals(
                "Number of successful allocations must not exceed the configured pool size",
                configuredPoolSize, successfulAcquisitions);

        // Verify threads beyond pool size respected the timeout
        for (int i = configuredPoolSize; i < totalThreads; i++) {
            Assert.assertFalse(
                    "Acquire should fail or timeout for threads exceeding pool capacity",
                    results[i]);
        }
    }
}