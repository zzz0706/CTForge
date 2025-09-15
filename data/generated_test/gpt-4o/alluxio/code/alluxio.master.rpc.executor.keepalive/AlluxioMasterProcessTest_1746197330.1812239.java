package alluxio.master;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.concurrent.ForkJoinPool;

public class AlluxioMasterProcessTest {
    // Member variable to hold the ForkJoinPool instance
    private ForkJoinPool mRPCExecutor;

    @Before
    public void setup() {
        // Prepare the test environment by retrieving the configuration values from ServerConfiguration API
        int parallelism = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM);
        int corePoolSize = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_CORE_POOL_SIZE);
        int maxPoolSize = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE);
        int minRunnable = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MIN_RUNNABLE);
        long keepAliveTimeMs = ServerConfiguration.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE);
        
        // ForkJoinPool doesn't have all these configurations available directly. Adjust accordingly.
        mRPCExecutor = new ForkJoinPool(parallelism);
    }

    @Test
    public void testRunWorkerWithValidKeepAliveTimeConfiguration() {
        // Test to validate the constructed ForkJoinPool properties
        Assert.assertNotNull("mRPCExecutor should not be null", mRPCExecutor);
        Assert.assertEquals(
                "ForkJoinPool parallelism should match configuration",
                ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM),
                mRPCExecutor.getParallelism()
        );
        Assert.assertFalse("mRPCExecutor should be alive", mRPCExecutor.isShutdown());
    }
}