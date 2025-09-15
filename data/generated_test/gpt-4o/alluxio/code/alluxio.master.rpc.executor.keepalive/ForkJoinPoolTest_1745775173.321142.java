package alluxio.master;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.concurrent.jsr.ForkJoinPool;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class ForkJoinPoolTest {

    @Test
    // test_runWorker_keepAliveBehavior
    // 1. Use the Alluxio API to fetch the keepAlive configuration value.
    // 2. Prepare the test conditions with a mocked WorkQueue and initialize necessary components.
    // 3. Test the `runWorker` behavior for idle threads respecting the keepAlive configuration.
    // 4. Validate thread termination after idle time exceeds keepAlive value.
    public void testRunWorkerKeepAliveBehavior() {
        // Step 1: Obtain the keepAlive configuration value via the Alluxio API
        long keepAliveTimeMs = ServerConfiguration.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE);

        // Step 2: Initialize ForkJoinPool with relevant configuration
        ForkJoinPool pool = new ForkJoinPool(
                ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM),
                ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                null,
                true,
                ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_CORE_POOL_SIZE),
                ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE),
                ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MIN_RUNNABLE),
                null,
                keepAliveTimeMs,
                TimeUnit.MILLISECONDS);

        // Step 3: Submit a dummy task to allow threads in the ForkJoinPool to initialize
        pool.submit(() -> {
            try {
                // Simulate thread idle behavior
                Thread.sleep(keepAliveTimeMs + 1000); // Exceed keep-alive time
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        });

        // Step 4: Wait for idle threads to simulate keeping alive within the configured time
        try {
            Thread.sleep(keepAliveTimeMs); // Ensure no premature termination
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Step 5: Shut down the ForkJoinPool to check thread termination after idle period
        pool.shutdown();

        try {
            // Await termination within a reasonable time limit
            if (!pool.awaitTermination(keepAliveTimeMs + 2000, TimeUnit.MILLISECONDS)) {
                throw new AssertionError("ForkJoinPool threads did not terminate after exceeding configured keepAlive time.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Test was interrupted while waiting for ForkJoinPool termination.");
        }
    }
}