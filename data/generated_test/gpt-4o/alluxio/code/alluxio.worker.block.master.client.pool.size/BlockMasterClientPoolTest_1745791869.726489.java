package alluxio.worker.block;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.resource.ResourcePool;
import alluxio.worker.block.BlockMasterClientPool;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;

public class BlockMasterClientPoolTest {

    @Test
    // test code
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testResourcePoolAcquireWithTimeout() throws Exception {
        // 1. Correctly obtain configuration values using the Alluxio 2.1.0 API
        int poolSize = ServerConfiguration.getInt(PropertyKey.WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE);

        // 2. Prepare the test conditions by creating a valid BlockMasterClientPool instance
        BlockMasterClientPool blockMasterClientPool = new BlockMasterClientPool();

        // 3. Test code: Simulate lack of available resources in the pool and test acquire with timeout
        for (int i = 0; i < poolSize; i++) {
            assertNotNull("Failed to acquire resources during pool creation.", blockMasterClientPool.acquire(0, null));
        }

        // Attempting to acquire a resource with a timeout should return null
        Object acquiredResource = blockMasterClientPool.acquire(100, TimeUnit.MILLISECONDS);
        assertNull("Expected acquire to return null when the pool is exhausted after timeout.", acquiredResource);

        // 4. Code after testing: Clean up the resources
        blockMasterClientPool.close();
    }

    @Test
    // test code
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testResourcePoolAcquireWithoutTimeout() throws Exception {
        // 1. Correctly obtain configuration values using the Alluxio 2.1.0 API
        int poolSize = ServerConfiguration.getInt(PropertyKey.WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE);

        // 2. Prepare the test conditions by creating a valid BlockMasterClientPool instance
        BlockMasterClientPool blockMasterClientPool = new BlockMasterClientPool();

        // 3. Test code: Simulate a scenario where resources exist and test acquire without timeout
        Object acquiredResource = blockMasterClientPool.acquire(0, null); // Acquire resource immediately
        assertNotNull("Expected acquire to return a resource when available.", acquiredResource);

        // Attempt to acquire additional resources within the pool capacity
        for (int i = 1; i < poolSize; i++) {
            assertNotNull("Expected acquire to return resources within the pool capacity.", blockMasterClientPool.acquire(0, null));
        }

        // 4. Code after testing: Clean up the resources
        blockMasterClientPool.close();
    }
}