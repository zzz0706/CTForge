package alluxio.worker.block;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.resource.ResourcePool;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

public class BlockMasterClientPoolTest {

    @Test
    public void test_BlockMasterClientPool_acquire_with_custom_timeout() throws Exception {
        // 1. Use the Alluxio 2.1.0 API to correctly obtain configuration values
        int poolSize = ServerConfiguration.getInt(PropertyKey.WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE);

        // 2. Prepare the test conditions
        // Initialize BlockMasterClientPool with the configuration value
        BlockMasterClientPool clientPool = new BlockMasterClientPool();

        // Simulate workload requiring concurrent client acquisition
        // Create threads exceeding the pool size to test capacity enforcement
        Thread[] threads = new Thread[poolSize + 2];
        boolean[] results = new boolean[threads.length];

        for (int i = 0; i < threads.length; i++) {
            final int index = i;
            threads[index] = new Thread(() -> {
                try {
                    // 3. Test Code
                    // Call acquire with a positive timeout (e.g., 100ms)
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

        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }

        // 4. Code after testing
        // Verify capacity enforcement and timeout adherence
        int successfulAcquisitions = 0;
        for (boolean result : results) {
            if (result) {
                successfulAcquisitions++;
            }
        }

        // Ensure that at most poolSize resources were acquired successfully
        Assert.assertEquals(poolSize, successfulAcquisitions);

        // Additional validation: Ensure resources beyond pool size respect timeout behavior
        for (int i = poolSize; i < threads.length; i++) {
            Assert.assertFalse(results[i]);
        }
    }
}