package alluxio.master;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.concurrent.jsr.ForkJoinPool;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public class ForkJoinPoolKeepAliveTest {

    @Test
    public void testForkJoinPoolConstructorKeepAliveValidation() {
        // Step 1: Prepare the test conditions
        long keepAliveTime = ServerConfiguration.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE);
        int parallelism = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM);
        int corePoolSize = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_CORE_POOL_SIZE);
        int maxPoolSize = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE);
        int minRunnable = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MIN_RUNNABLE);

        ForkJoinPool.ForkJoinWorkerThreadFactory threadFactory = ForkJoinPool.defaultForkJoinWorkerThreadFactory;
        boolean asyncMode = true;
        Predicate<? super ForkJoinPool> saturate = null;

        // Step 2: Test code
        // Verify normal instantiation with valid keep-alive time
        ForkJoinPool pool = new ForkJoinPool(
                parallelism,
                threadFactory,
                null, 
                asyncMode,
                corePoolSize,
                maxPoolSize,
                minRunnable,
                saturate,
                keepAliveTime,
                TimeUnit.MILLISECONDS);

        // Verify that the pool correctly uses the keepAlive value
        assert pool != null;
        assert pool.getParallelism() == parallelism;

        // Step 3: Prepare and test invalid keepAliveTime to trigger IllegalArgumentException
        try {
            new ForkJoinPool(
                    parallelism,
                    threadFactory,
                    null, 
                    asyncMode,
                    corePoolSize,
                    maxPoolSize,
                    minRunnable,
                    saturate,
                    -1, // Invalid keepAliveTime
                    TimeUnit.MILLISECONDS);
            assert false; // This statement must not be reached
        } catch (IllegalArgumentException e) {
            // Expected, test passes
        }

        // Step 4: Code after testing (resource cleanup)
        pool.shutdown();
    }
}