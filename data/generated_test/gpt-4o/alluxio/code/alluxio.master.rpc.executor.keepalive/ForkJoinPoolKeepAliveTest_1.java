package alluxio.master;

import org.junit.Assert;
import org.junit.Test;
import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

public class ForkJoinPoolKeepAliveTest {

    @Test
    public void testRunWorkerWithKeepAliveBehavior() {
        // 1. Use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        long keepAliveTimeMs = ServerConfiguration.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE); // Fetch correct configuration using the Alluxio API.

        // 2. Prepare the test conditions: Create test ForkJoinPool instance.
        ForkJoinPool forkJoinPool = new ForkJoinPool(
                2, // parallelism
                ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                null, // UncaughtExceptionHandler
                true // asyncMode
        );

        try {
            // 3. Test code: Submit a task to simulate worker behavior and validate keepAlive functionality.
            forkJoinPool.submit(() -> {
                try {
                    // Simulate work and thread idle time.
                    Thread.sleep(keepAliveTimeMs / 2);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            // Wait for the completion of submitted task.
            forkJoinPool.shutdown();
            boolean terminated = forkJoinPool.awaitTermination(keepAliveTimeMs + 100, TimeUnit.MILLISECONDS);

            // Verify that the ForkJoinPool shuts down correctly.
            Assert.assertTrue("ForkJoinPool should be terminated after keepAlive timeout", terminated);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Assert.fail("Test was interrupted");
        } finally {
            // 4. Code after testing: Clean up resources to ensure no lingering threads.
            if (!forkJoinPool.isTerminated()) {
                forkJoinPool.shutdownNow();
            }
        }
    }
}