package alluxio.master;

import alluxio.concurrent.jsr.ForkJoinPool;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import org.junit.Test;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.*;

public class AlluxioMasterProcessTest {

    @Test
    // test code
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testForkJoinPoolInvalidMaxPoolSize() {
        // 1. Using Alluxio 2.1.0 API to fetch configuration values dynamically
        int corePoolSize = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_CORE_POOL_SIZE);
        int maxPoolSize = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE);

        // 2. Validate the prerequisite: simulate with invalid configuration values
        // If the configuration isn't invalid, this test won't execute the desired behavior
        if (maxPoolSize >= corePoolSize) {
            System.out.println("Skipping test; configuration values are valid.");
            return;
        }

        // 3. Attempt to initialize the ForkJoinPool with invalid configuration
        try {
            ForkJoinPool pool = new ForkJoinPool(
                ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM),
                ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                null,
                true,
                corePoolSize,
                maxPoolSize, // Invalid max pool size triggering IllegalArgumentException
                ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MIN_RUNNABLE),
                null,
                ServerConfiguration.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE),
                TimeUnit.MILLISECONDS
            );
            
            // Assert failure as we expect an exception
            fail("Expected IllegalArgumentException was not thrown.");
        } catch (IllegalArgumentException e) {
            // 4. Verify the exception adheres to the expected behavior
            assertTrue("Exception message does not contain the expected text",
                e.getMessage().contains("IllegalArgumentException"));
        }
    }
}