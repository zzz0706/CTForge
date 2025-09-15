package alluxio.master;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.concurrent.jsr.ForkJoinPool;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public class AlluxioMasterConfigTest {
    private ForkJoinPool mRPCExecutor;

    @Before
    public void setup() {
        // Initialization of required components
    }

    @Test
    public void testForkJoinPoolConstructorKeepAliveValidation() {
        // 1. Obtain configuration values dynamically using Alluxio API
        long keepAliveTime = ServerConfiguration.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE);
        int parallelism = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM);
        int corePoolSize = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_CORE_POOL_SIZE);
        int maxPoolSize = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE);
        int minRunnable = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MIN_RUNNABLE);

        ForkJoinPool.ForkJoinWorkerThreadFactory threadFactory = ForkJoinPool.defaultForkJoinWorkerThreadFactory;
        boolean asyncMode = true;
        Predicate<? super ForkJoinPool> saturate = null;

        // 2. Prepare the test conditions and test normal instantiation with valid keepAliveTime
        mRPCExecutor = new ForkJoinPool(
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

        // 3. Test code and verify results
        assert mRPCExecutor != null;
        assert mRPCExecutor.getParallelism() == parallelism;

        // Test invalid keepAliveTime (-1) to trigger IllegalArgumentException
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
            assert false; // If this line is reached, the test fails
        } catch (IllegalArgumentException e) {
            // Expected exception, test passes
        }

        // 4. Code after testing (resource cleanup)
        mRPCExecutor.shutdown();
    }
}