package alluxio.master;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.concurrent.jsr.ForkJoinPool;
import org.junit.Test;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.*;

public class AlluxioMasterProcessTest {

    @Test
    public void Test_ForkJoinPool_InvalidMaxPoolSize() {
        // Test code
        // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        int corePoolSize = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_CORE_POOL_SIZE);
        int maxPoolSize = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE);
        
        // 2. Prepare the test conditions.
        // Validate that the maximum pool size is less than the core pool size to simulate invalid configuration.
        if (maxPoolSize >= corePoolSize) {
            // Skip this test if the configuration values are valid, as we're testing invalid cases.
            System.out.println("Skipping test as configuration values are valid.");
            return;
        }

        // 3. Test code.
        try {
            ForkJoinPool pool = new ForkJoinPool(
                ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM),
                ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                null,
                true,
                corePoolSize,
                maxPoolSize, // This should trigger an IllegalArgumentException
                ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MIN_RUNNABLE),
                null,
                ServerConfiguration.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE),
                TimeUnit.MILLISECONDS
            );
            fail("Expected IllegalArgumentException was not thrown.");
        } catch (IllegalArgumentException e) {
            // 4. Code after testing.
            // Verify the exception is as expected.
            assertTrue("Exception message does not contain expected text", 
                e.getMessage().contains("IllegalArgumentException"));
        }
    }
}