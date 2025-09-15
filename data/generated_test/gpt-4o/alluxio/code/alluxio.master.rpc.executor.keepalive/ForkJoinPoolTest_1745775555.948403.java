package alluxio.master;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.concurrent.jsr.ForkJoinPool;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class ForkJoinPoolTest {

    @Test
    public void testRunWorker_nonIdleThreadBehavior() {
        // Step 1: Fetch the configuration value for keepAlive using Alluxio's ServerConfiguration API
        long keepAliveTime = ServerConfiguration.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE);

        // Step 2: Initialize the ForkJoinPool with the configuration value
        ForkJoinPool pool = new ForkJoinPool(
            2, // parallelism
            ForkJoinPool.defaultForkJoinWorkerThreadFactory,
            null, // no UncaughtExceptionHandler
            true, // asyncMode
            2, // corePoolSize
            4, // maximumPoolSize
            1, // minimumRunnable
            null, // saturate predicate
            keepAliveTime, // keepAliveTime fetched from configuration
            TimeUnit.MILLISECONDS
        );

        // Note: ForkJoinPool.WorkQueue is not accessible as it is not public, so we cannot 
        // mock or operate directly on it.
        // Instead, we can simulate the expected behavior through other means,
        // such as submitting tasks to the pool and verifying its state.

        // Step 3: Submit a task to the ForkJoinPool
        pool.submit(() -> {
            // Simulated task
            try {
                TimeUnit.MILLISECONDS.sleep(keepAliveTime / 2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // Step 4: Verify the pool behavior through its metrics or state
        // Ensure the pool remains active during the task execution
        assert pool.getActiveThreadCount() > 0 : "No active threads in the pool";

        // Shut down the pool after use
        pool.shutdown();
    }
}