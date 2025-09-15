package alluxio.master;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ForkJoinPool;

public class AlluxioMasterProcessTest {

    @Test
    // test code
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testForkJoinPoolInitializationWithConfig() {
        // 1. Obtain configuration values using ServerConfiguration API.
        int parallelism = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM);

        // 2. Prepare the ForkJoinPool with the configuration values for testing.
        ForkJoinPool forkJoinPool = new ForkJoinPool(parallelism);

        // 3. Test the created ForkJoinPool using assertions.
        Assert.assertNotNull("ForkJoinPool should be initialized", forkJoinPool);
        Assert.assertEquals("Parallelism should match configured value", parallelism, forkJoinPool.getParallelism());

        // 4. Shutdown ForkJoinPool and perform assertions.
        forkJoinPool.shutdown();
        Assert.assertTrue("ForkJoinPool should be shut down", forkJoinPool.isShutdown());
    }
}