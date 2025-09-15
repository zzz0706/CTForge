package alluxio.master;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.concurrent.jsr.ForkJoinPool;
import org.junit.Test;
import org.junit.Assert;

import java.util.concurrent.TimeUnit;

public class ForkJoinPoolTest {

    @Test
    // test code
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_ForkJoinPool_constructor_maximumPoolSize() {
        // Step 1: Obtain relevant configuration values using Alluxio API
        int parallelism = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM);
        int corePoolSize = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_CORE_POOL_SIZE);
        int maximumPoolSize = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE);
        int minimumRunnable = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MIN_RUNNABLE);
        long keepAliveTime = ServerConfiguration.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE);

        // Validate IllegalArgumentException is thrown for invalid inputs
        try {
            // Invalid maximumPoolSize - less than parallelism
            new ForkJoinPool(
                parallelism,
                ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                null,
                true,
                corePoolSize,
                parallelism - 1, // Invalid maximum pool size
                minimumRunnable,
                null,
                keepAliveTime,
                TimeUnit.MILLISECONDS
            );
            Assert.fail("Exception was expected due to invalid maximumPoolSize.");
        } catch (IllegalArgumentException expected) {
            // Exception is expected, test passes
        }

        try {
            // Invalid maximumPoolSize - negative value
            new ForkJoinPool(
                parallelism,
                ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                null,
                true,
                corePoolSize,
                -1, // Invalid maximum pool size
                minimumRunnable,
                null,
                keepAliveTime,
                TimeUnit.MILLISECONDS
            );
            Assert.fail("Exception was expected due to invalid maximumPoolSize.");
        } catch (IllegalArgumentException expected) {
            // Exception is expected, test passes
        }

        // Step 2: Validate normal behavior for valid inputs
        try {
            ForkJoinPool pool = new ForkJoinPool(
                parallelism,
                ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                null,
                true,
                corePoolSize,
                maximumPoolSize, // Valid maximum pool size
                minimumRunnable,
                null,
                keepAliveTime,
                TimeUnit.MILLISECONDS
            );

            // Verify instance is created successfully
            Assert.assertNotNull(pool);
        } catch (IllegalArgumentException e) {
            Assert.fail("Exception should not be thrown for valid configuration values.");
        }
    }
}