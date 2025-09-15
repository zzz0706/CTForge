package alluxio.master;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.concurrent.jsr.ForkJoinPool;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class ForkJoinPoolUsageTest {

    @Test
    // Test code
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testForkJoinPoolIdleThreadTermination() throws InterruptedException {
        // 1. Dynamically fetch configuration values using Alluxio API
        AlluxioConfiguration configuration = ServerConfiguration.global();
        int parallelism = configuration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM);
        int corePoolSize = configuration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_CORE_POOL_SIZE);
        int maxPoolSize = configuration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE);
        long keepAliveTime = configuration.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE);

        // 2. Create a ForkJoinPool instance using the fetched configuration values
        ForkJoinPool forkJoinPool = new ForkJoinPool(
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

        // Simulate workload tasks to initialize worker threads
        for (int i = 0; i < 10; i++) {
            forkJoinPool.execute(() -> {
                try {
                    Thread.sleep(200); // Simulate a task executing
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        // Ensure tasks are finished
        forkJoinPool.awaitQuiescence(1000, TimeUnit.MILLISECONDS);

        // Allow ForkJoinPool to reach idle state
        Thread.sleep(keepAliveTime + 500);

        // 3. Validate termination of idle threads
        int activeThreadCount = forkJoinPool.getActiveThreadCount();
        int poolSize = forkJoinPool.getPoolSize();

        Assert.assertTrue("Idle threads should terminate correctly after the keepAliveTime threshold",
            poolSize > activeThreadCount);

        // 4. Shutdown the ForkJoinPool after test execution
        forkJoinPool.shutdown();
    }
}