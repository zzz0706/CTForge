package alluxio.worker.block;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
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
        // 1. Use the Alluxio 2.1.0 API to correctly obtain configuration values
        int poolSize = ServerConfiguration.getInt(PropertyKey.WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE);

        // 2. Prepare the test conditions
        // Set the configuration specific to our test scope
        ServerConfiguration.set(PropertyKey.WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE, Integer.toString(poolSize));

        // Create the BlockMasterClientPool instance initialized with the configuration value
        BlockMasterClientPool clientPool = new BlockMasterClientPool();

        // Prepare test threads to simulate workload requiring concurrent client acquisition
        int additionalThreads = 2; // Simulating more threads than pool size to test capacity
        int totalThreads = poolSize + additionalThreads;
        Thread[] threads = new Thread[totalThreads];
        boolean[] results = new boolean[totalThreads];

        for (int i = 0; i < totalThreads; i++) {
            final int index = i;
            threads[index] = new Thread(() -> {
                try {
                    // 3. Test Code
                    // Call acquire with a positive timeout (100ms in this case)
                    Object resource = clientPool.acquire(100, TimeUnit.MILLISECONDS);
                    results[index] = resource != null; // Successful acquisition
                } catch (Exception e) {
                    results[index] = false; // Handle exceptions during acquisition
                }
            });
        }

        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }

        // Wait for all threads to finish execution
        for (Thread thread : threads) {
            thread.join();
        }

        // 4. Code after testing
        // Verify capacity enforcement and timeout behavior
        int successfulAcquisitions = 0;
        for (boolean result : results) {
            if (result) {
                successfulAcquisitions++;
            }
        }

        // Ensure that at most poolSize resources were acquired successfully
        Assert.assertEquals("Number of successful allocations must not exceed pool size",
                poolSize, successfulAcquisitions);

        // Validate that threads beyond pool size respected timeout behavior
        for (int i = poolSize; i < totalThreads; i++) {
            Assert.assertFalse("Acquire should fail or timeout for threads beyond capacity", results[i]);
        }
    }
}