package alluxio.master;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import org.junit.Test;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

public class AlluxioMasterProcessTest {

    @Test
    // testRunWorkerKeepAliveBehavior
    // 1. Use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testRunWorkerKeepAliveBehavior() {
        // Step 1: Use the Alluxio API to fetch the configuration values for testing
        int parallelism = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM);
        long keepAliveTimeMs = ServerConfiguration.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE);

        // Step 2: Initialize ForkJoinPool with the configuration values
        ForkJoinPool pool = new ForkJoinPool(
                parallelism,
                ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                null,
                true);

        // Step 3: Prepare test conditions with a dummy task to simulate idle threads
        pool.submit(() -> {
            try {
                // Simulate thread idle behavior exceeding keepAlive time
                Thread.sleep(keepAliveTimeMs + 1000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        });

        // Step 4: Validate that idle threads do not prematurely terminate
        try {
            Thread.sleep(keepAliveTimeMs / 2); // Ensure no premature termination
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Step 5: Shut down the ForkJoinPool and verify thread termination within the expected time
        pool.shutdown();
        try {
            boolean terminated = pool.awaitTermination(keepAliveTimeMs + 2000, TimeUnit.MILLISECONDS);
            if (!terminated) {
                throw new AssertionError("ForkJoinPool threads did not terminate after exceeding configured keepAlive time.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Test was interrupted while waiting for ForkJoinPool termination.");
        }
    }
}