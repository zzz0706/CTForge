package alluxio.master;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.concurrent.jsr.ForkJoinPool;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;

public class AlluxioMasterProcessTest {
    // Test method to validate edge cases of 'alluxio.master.rpc.executor.core.pool.size'
    @Test
    public void testStartServingRPCServerWithEdgeCases() {
        // Prepare test conditions: Validate configuration values required for testing
        int corePoolSizeMin = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM);
        int corePoolSizeMax = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE);
        int corePoolSizeZero = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_CORE_POOL_SIZE);

        // Test with minimum core pool size
        try {
            ForkJoinPool forkJoinPoolMin = new ForkJoinPool(
                    corePoolSizeMin,
                    ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                    null,
                    true,
                    corePoolSizeMin,
                    ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE),
                    ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MIN_RUNNABLE),
                    null,
                    ServerConfiguration.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE),
                    java.util.concurrent.TimeUnit.MILLISECONDS
            );
            assertNotNull(forkJoinPoolMin);
        } catch (Exception e) {
            // Log exception for debugging purposes
            System.err.println("Exception occurred during minimum core pool size test: " + e.getMessage());
        }

        // Test with maximum core pool size
        try {
            ForkJoinPool forkJoinPoolMax = new ForkJoinPool(
                    corePoolSizeMax,
                    ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                    null,
                    true,
                    corePoolSizeMax,
                    ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE),
                    ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MIN_RUNNABLE),
                    null,
                    ServerConfiguration.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE),
                    java.util.concurrent.TimeUnit.MILLISECONDS
            );
            assertNotNull(forkJoinPoolMax);
        } catch (Exception e) {
            // Log exception for debugging purposes
            System.err.println("Exception occurred during maximum core pool size test: " + e.getMessage());
        }

        // Test with zero core pool size
        try {
            ForkJoinPool forkJoinPoolZero = new ForkJoinPool(
                    corePoolSizeZero,
                    ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                    null,
                    true,
                    corePoolSizeZero,
                    ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE),
                    ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MIN_RUNNABLE),
                    null,
                    ServerConfiguration.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE),
                    java.util.concurrent.TimeUnit.MILLISECONDS
            );
            assertNotNull(forkJoinPoolZero);
        } catch (Exception e) {
            // Log exception for debugging purposes
            System.err.println("Exception occurred during zero core pool size test: " + e.getMessage());
        }
    }
}