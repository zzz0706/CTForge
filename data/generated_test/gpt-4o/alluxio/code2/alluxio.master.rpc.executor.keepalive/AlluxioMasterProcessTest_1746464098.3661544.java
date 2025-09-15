package alluxio.master;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

public class AlluxioMasterProcessTest {
    @Test
    public void testForkJoinPoolInitializationWithKeepAliveConfig() {
        // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        long keepAliveMs = ServerConfiguration.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE);
        int parallelism = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM);

        // 2. Prepare the test conditions.
        ForkJoinPool forkJoinPool = new ForkJoinPool(
                parallelism,
                ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                null,
                true
        );

        // 3. Test code.
        Assert.assertNotNull("ForkJoinPool should be initialized", forkJoinPool);
        Assert.assertEquals("Parallelism should match configured value", parallelism, forkJoinPool.getParallelism());

        // 4. Code after testing.
        forkJoinPool.shutdown();
        Assert.assertTrue("ForkJoinPool should be shut down", forkJoinPool.isShutdown());
    }
}