package alluxio.master;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Test;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

public class ForkJoinPoolTest {
    /**
     * Test case: test_runWorker_keepAliveBehavior
     * Objective: Confirm that the ForkJoinPool respects the configured keepAlive value
     * when deciding to terminate idle threads.
     */
    @Test
    public void testRunWorkerKeepAliveBehavior() {
        // Step 1: Fetch the keepAlive configuration value using the Alluxio API
        long keepAliveTime = ServerConfiguration.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE);

        // Step 2: Prepare the ForkJoinPool instance with the fetched keepAlive value
        ForkJoinPool pool = new ForkJoinPool(
                Runtime.getRuntime().availableProcessors(), 
                ForkJoinPool.defaultForkJoinWorkerThreadFactory, 
                null,
                true);

        // Step 3: Submit a dummy task to allow threads in the ForkJoinPool to initialize
        pool.submit(() -> {
            try {
                Thread.sleep(keepAliveTime + 1000); // Simulate work exceeding keepAlive time
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        });

        // Step 4: Wait for keepAliveTime to ensure the threads stay alive
        try {
            Thread.sleep(keepAliveTime);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Final Step: Shut down the ForkJoinPool
        pool.shutdown();

        try {
            // Wait for the termination to verify threads exit
            if (!pool.awaitTermination(keepAliveTime + 2000, TimeUnit.MILLISECONDS)) {
                throw new AssertionError("ForkJoinPool threads did not terminate within the expected keepAlive time.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Test was interrupted while waiting for ForkJoinPool to terminate.");
        }
    }
}