package alluxio.master;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.concurrent.jsr.ForkJoinPool;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;

public class ForkJoinPoolTest {

    /**
     * Test the behavior of ForkJoinPool's `runWorker` method under varying workloads 
     * while adhering to the configured thread pool size constraints.
     */
    @Test
    public void testForkJoinPoolRunWorkerBehavior() {
        // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        int parallelism = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM);
        int corePoolSize = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_CORE_POOL_SIZE);
        int maxPoolSize = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE);
        int minimumRunnable = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MIN_RUNNABLE);
        long keepAliveTime = ServerConfiguration.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE);

        // 2. Prepare the test conditions.
        AtomicInteger processedTasks = new AtomicInteger(0);

        // Create a ForkJoinPool instance with the configuration values obtained from API
        ForkJoinPool pool = new ForkJoinPool(
            parallelism,
            ForkJoinPool.defaultForkJoinWorkerThreadFactory,
            null,
            true,
            corePoolSize,
            maxPoolSize,
            minimumRunnable,
            null,
            keepAliveTime,
            TimeUnit.MILLISECONDS
        );

        // Normal workload: Submit tasks below maxPoolSize
        for (int i = 0; i < maxPoolSize / 2; i++) {
            pool.submit(() -> {
                processedTasks.incrementAndGet();
                try {
                    Thread.sleep(100); // Simulate task execution
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        // 3. Test code for burst workload: Submit excessive tasks exceeding maxPoolSize
        for (int i = 0; i < maxPoolSize * 2; i++) {
            pool.submit(() -> {
                processedTasks.incrementAndGet();
                try {
                    Thread.sleep(100); // Simulate task execution
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        // Wait until tasks complete
        pool.awaitQuiescence(1, TimeUnit.MINUTES);

        // 4. Code after testing: Assert that the active threads do not exceed maxPoolSize
        assertTrue(pool.getPoolSize() <= maxPoolSize);
        assertTrue(processedTasks.get() > 0); // Verify some tasks were processed
    }
}