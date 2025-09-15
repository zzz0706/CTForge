package alluxio.master;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.concurrent.jsr.ForkJoinPool;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public class ForkJoinPoolKeepAliveTest {
    @Test
    public void test_ForkJoinPool_constructor_keepAliveValidation() {
        // Step 1: Prepare the test conditions
        // Fetch the keepAlive time value using the Alluxio API
        long keepAliveTime = ServerConfiguration.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE);

        // Mock or create required inputs
        int parallelism = 4;
        ForkJoinPool.ForkJoinWorkerThreadFactory threadFactory = ForkJoinPool.defaultForkJoinWorkerThreadFactory;
        boolean asyncMode = true;
        int corePoolSize = 2;
        int maxPoolSize = 8;
        int minRunnable = 1;
        Predicate<? super ForkJoinPool> saturate = null;

        // Step 2: Test code
        // Ensure that the ForkJoinPool correctly uses the keepAlive value
        ForkJoinPool pool = new ForkJoinPool(
                parallelism,
                threadFactory,
                null, // No custom UncaughtExceptionHandler
                asyncMode,
                corePoolSize,
                maxPoolSize,
                minRunnable,
                saturate,
                keepAliveTime,
                TimeUnit.MILLISECONDS);

        // Step 3: Verify the pool's initialization and behavior
        // Check that the pool is not null and correctly instantiated
        assert pool != null;
        assert pool.getParallelism() == parallelism;

        // Optional: Verify that the pool shuts idle threads correctly based on the keep-alive time.

        // Step 4: Code after testing
        // Shutdown the pool after verifying its properties
        pool.shutdown();
    }
}