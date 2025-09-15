package alluxio.master;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.concurrent.jsr.ForkJoinPool;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class AlluxioMasterProcessTest {

    @Test
    public void test_ForkJoinPool_maxPoolSizeEffectUnderLoad() throws InterruptedException {
        // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values.
        int parallelism;
        int maximumPoolSize;
        long keepAliveTime;

        try {
            parallelism = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM);
            if (parallelism <= 0) {
                throw new IllegalArgumentException("Invalid parallelism configuration value.");
            }

            maximumPoolSize = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE);
            if (maximumPoolSize <= 0) {
                throw new IllegalArgumentException("Invalid maximum pool size configuration value.");
            }

            keepAliveTime = ServerConfiguration.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE);
            if (keepAliveTime <= 0) {
                throw new IllegalArgumentException("Invalid keepAliveTime configuration value.");
            }
        } catch (Exception e) {
            throw new IllegalStateException("Configuration values retrieval failed, ensure properties are set correctly", e);
        }

        // 2. Prepare the test conditions.
        ForkJoinPool executorPool = new ForkJoinPool(
            parallelism,
            ForkJoinPool.defaultForkJoinWorkerThreadFactory,
            null,
            true,
            parallelism,
            maximumPoolSize,
            0,
            null,
            keepAliveTime,
            TimeUnit.MILLISECONDS
        );

        AtomicInteger taskCompletionCount = new AtomicInteger(0);

        try {
            // 3. Submit multiple tasks to the pool to simulate heavy concurrent requests.
            int taskCount = maximumPoolSize * 2;  // Ensuring enough tasks to potentially reach the maximum pool size.
            for (int i = 0; i < taskCount; i++) {
                executorPool.execute(() -> {
                    try {
                        // Simulate a task workload.
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        // Handle thread interruption.
                        Thread.currentThread().interrupt();
                    }
                    taskCompletionCount.incrementAndGet();
                });
            }

            // Wait for tasks to complete or the pool to shut down within a reasonable timeout.
            // Added longer timeout to handle potential delays under heavy load more gracefully.
            executorPool.shutdown();
            boolean terminated = executorPool.awaitTermination(10, TimeUnit.SECONDS);

            // 4. Code after testing: Assert correctness.
            Assert.assertTrue(
                "Executor pool should terminate all tasks successfully within timeout.",
                terminated
            );

            Assert.assertTrue(
                "Number of active threads in the pool should not exceed maximumPoolSize during execution.",
                executorPool.getPoolSize() <= maximumPoolSize
            );

            Assert.assertEquals(
                "All submitted tasks should complete execution.",
                taskCount,
                taskCompletionCount.get()
            );
        } finally {
            if (!executorPool.isTerminated()) {
                // Forcefully shutdown the pool to clean up resources in case of failure.
                executorPool.shutdownNow();
            }
        }
    }
}