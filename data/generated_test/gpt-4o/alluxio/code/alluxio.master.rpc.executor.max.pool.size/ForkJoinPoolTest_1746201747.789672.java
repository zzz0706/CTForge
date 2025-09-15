package alluxio.master;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.concurrent.jsr.ForkJoinPool;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class ForkJoinPoolTest {

    // Test case: testForkJoinPoolIdleThreadTermination
    // Objective: Validate that idle threads in the `ForkJoinPool` are terminated correctly based on the `keepAliveTime` configuration.

    @Test
    public void testForkJoinPoolIdleThreadTermination() throws InterruptedException {
        // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values.
        AlluxioConfiguration configuration = ServerConfiguration.global();

        int parallelism = configuration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM);
        int corePoolSize = configuration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_CORE_POOL_SIZE);
        int maxPoolSize = configuration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE);
        long keepAliveTime = configuration.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE);

        // 2. Prepare the test conditions.
        ForkJoinPool pool = new ForkJoinPool(
            parallelism,
            ForkJoinPool.defaultForkJoinWorkerThreadFactory,
            null,
            true,
            corePoolSize,
            maxPoolSize,
            0,
            null,
            keepAliveTime,
            TimeUnit.MILLISECONDS
        );

        // Run workload tasks to initialize worker threads
        for (int i = 0; i < 10; i++) {
            pool.execute(() -> {
                try {
                    Thread.sleep(100); // Simulating a task
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        // Wait for pool activity to cease
        pool.awaitQuiescence(500, TimeUnit.MILLISECONDS);

        // Ensure pool is idle for the `keepAliveTime` threshold
        Thread.sleep(keepAliveTime + 500);

        // 3. Test code: Verify that idle threads are terminated correctly.
        int activeThreadCount = pool.getActiveThreadCount();
        int poolSize = pool.getPoolSize();

        Assert.assertTrue("Idle threads should terminate correctly after the keepAliveTime threshold", poolSize > activeThreadCount);

        // 4. Code after testing.
        pool.shutdown();
    }
}