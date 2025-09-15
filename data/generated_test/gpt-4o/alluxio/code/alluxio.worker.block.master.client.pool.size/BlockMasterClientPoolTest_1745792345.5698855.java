package alluxio.worker.block;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.resource.ResourcePool;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
        // Instantiate BlockMasterClientPool, initializing it with the configuration
        BlockMasterClientPool clientPool = new BlockMasterClientPool();

        // Simulate concurrent workload exceeding pool capacity
        int additionalThreads = 3; // Number of extra client threads
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
                    results[threadIndex] = (resource != null); // Mark true if resource acquired
                } catch (Exception e) {
                    results[threadIndex] = false; // Handle any acquisition failures
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

        // Step 4: Code after testing
        // Verify that successful allocations do not exceed configured pool capacity
        int successfulAcquisitions = 0;
        for (boolean result : results) {
            if (result) {
                successfulAcquisitions++;
            }
        }

        // Assertion: Ensure successful allocations match the configured pool size
        Assert.assertEquals(
                "Number of successful acquisitions should match the configured pool size",
                configuredPoolSize, successfulAcquisitions);

        // Assertion: Validate threads exceeding pool capacity respected the timeout
        for (int i = configuredPoolSize; i < totalThreads; i++) {
            Assert.assertFalse(
                    "Threads exceeding pool capacity should fail to acquire a resource within the timeout",
                    results[i]);
        }
    }
}