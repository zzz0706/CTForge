package alluxio.worker.block;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.resource.ResourcePool;
import org.junit.Assert;
import org.junit.Test;
import java.util.concurrent.TimeUnit;

public class BlockMasterClientPoolTest {

    @Test
    public void test_BlockMasterClientPool_acquire_with_custom_timeout() throws Exception {
        // 1. Obtain the configuration value for alluxio.worker.block.master.client.pool.size
        int poolSize = ServerConfiguration.getInt(PropertyKey.WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE);

        // 2. Initialize BlockMasterClientPool
        BlockMasterClientPool clientPool = new BlockMasterClientPool();

        // 3. Simulate workload requiring concurrent client acquisition
        // Here we use threads to simulate concurrent resource acquisition
        Thread[] threads = new Thread[poolSize + 1];
        boolean[] results = new boolean[threads.length];

        for (int i = 0; i < threads.length; i++) {
            final int index = i;
            threads[i] = new Thread(() -> {
                try {
                    // 4. Call acquire on BlockMasterClientPool with timeout (e.g., 100ms)
                    Object resource = clientPool.acquire(100, TimeUnit.MILLISECONDS);
                    results[index] = resource != null; // Check if resource is successfully acquired
                } catch (Exception e) {
                    results[index] = false;
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

        // 5. Verify results: Check capacity enforcement and timeout behavior
        int successfulAcquisitions = 0;
        for (boolean result : results) {
            if (result) {
                successfulAcquisitions++;
            }
        }

        // Verify that the pool enforces capacity constraints (only `poolSize` resources should be acquired)
        Assert.assertEquals(poolSize, successfulAcquisitions);
    }
}