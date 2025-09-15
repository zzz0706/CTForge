package alluxio.master;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.concurrent.jsr.ForkJoinPool;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class AlluxioMasterProcessTest {
    // Test case for verifying the correct behavior of the `runWorker` method in ForkJoinPool
    @Test
    public void testRunWorkerHandlesConcurrentTasksCorrectly() throws Exception {
        // Retrieve the `max.pool.size` configuration value dynamically
        int maxPoolSize = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE);

        // Prepare parameters for the ForkJoinPool
        int parallelism = 4; // Assume a parallelism level for the test
        int corePoolSize = 2; // Core pool size to keep alive
        long keepAliveTime = 10; // Keep alive time for idle threads
        TimeUnit timeUnit = TimeUnit.SECONDS;
        int minimumRunnable = 1; // Minimum runnable tasks for throttling

        // Create a ForkJoinPool using the retrieved and prepared parameters
        ForkJoinPool pool = new ForkJoinPool(
                parallelism,
                ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                null,
                true, // FIFO mode
                corePoolSize,
                maxPoolSize,
                minimumRunnable,
                null,
                keepAliveTime,
                timeUnit
        );

        try {
            // Submit tasks with varying computation durations to the pool
            for (int i = 0; i < 10; i++) {
                final int taskId = i;
                pool.submit(() -> {
                    // Simulate a computation task
                    try {
                        Thread.sleep(100); // Task processing for 100ms
                        System.out.println("Task " + taskId + " completed");
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }

            // Wait for tasks to complete
            pool.awaitQuiescence(1, TimeUnit.MINUTES);

            // Ensure all tasks were processed without deadlocks or starvation
            assertTrue(pool.isQuiescent());
        } finally {
            // Shutdown the pool after test
            pool.shutdown();
        }
    }
}