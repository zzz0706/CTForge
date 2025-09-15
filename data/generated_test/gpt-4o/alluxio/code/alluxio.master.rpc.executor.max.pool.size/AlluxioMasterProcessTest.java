package alluxio.master;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.concurrent.jsr.ForkJoinPool;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class AlluxioMasterProcessTest {
    @Test
    public void testForkJoinPool_ValidMaxPoolSize() {
        // 1. Use the Alluxio 2.1.0 API to get configuration values dynamically
        int maxPoolSize = ServerConfiguration.global().getInt(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE);
        int corePoolSize = ServerConfiguration.global().getInt(PropertyKey.MASTER_RPC_EXECUTOR_CORE_POOL_SIZE);
        int parallelism = ServerConfiguration.global().getInt(PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM);
        int minRunnable = ServerConfiguration.global().getInt(PropertyKey.MASTER_RPC_EXECUTOR_MIN_RUNNABLE);
        long keepAliveTime = ServerConfiguration.global().getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE);

        // 2. Prepare test-specific conditions
        ForkJoinPool forkJoinPool = null;
        try {
            // 3. Test code – create ForkJoinPool instance
            forkJoinPool = new ForkJoinPool(
                    parallelism,
                    ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                    null,
                    true,  // asyncMode
                    corePoolSize,
                    maxPoolSize,
                    minRunnable,
                    null,
                    keepAliveTime,
                    TimeUnit.MILLISECONDS);

            // 4. Validate some basic properties of the ForkJoinPool
            Assert.assertNotNull("ForkJoinPool should be successfully created", forkJoinPool);
            Assert.assertEquals("Parallelism should match the configured value", parallelism, forkJoinPool.getParallelism());
            Assert.assertTrue("Maximum pool size should be greater than or equal to core pool size",
                    maxPoolSize >= corePoolSize);
        } finally {
            // 5. Code after testing - shutdown the pool if it was created
            if (forkJoinPool != null) {
                forkJoinPool.shutdown();
            }
        }
    }
}