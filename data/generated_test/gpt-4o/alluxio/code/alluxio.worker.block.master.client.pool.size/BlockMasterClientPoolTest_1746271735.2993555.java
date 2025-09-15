package alluxio.worker.block;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.worker.block.BlockMasterClient;
import alluxio.worker.block.BlockMasterClientPool;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class BlockMasterClientPoolTest {

    @Test
    public void testBlockMasterClientPoolAcquireResourceSuccess() {
        // 1. Use the Alluxio 2.1.0 API to obtain configuration values
        int maxCapacity = ServerConfiguration.getInt(PropertyKey.WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE);
        Assert.assertTrue("Max capacity should be greater than 0", maxCapacity > 0);

        // 2. Prepare the test conditions
        // Create a BlockMasterClientPool instance (which automatically uses the configuration value for max capacity)
        BlockMasterClientPool clientPool = new BlockMasterClientPool();

        // 3. Test code
        // Attempt to acquire a resource from the pool with sufficient resources available
        BlockMasterClient resource = clientPool.acquire(1000, TimeUnit.MILLISECONDS);

        // Assert that the resource was successfully acquired
        Assert.assertNotNull("A valid resource should be acquired from the pool", resource);

        // Check that no more resources than maxCapacity are created (simulate multiple requests if necessary)
        for (int i = 0; i < maxCapacity - 1; i++) {
            BlockMasterClient additionalResource = clientPool.acquire(1000, TimeUnit.MILLISECONDS);
            Assert.assertNotNull("Resource should still be acquirable before reaching max capacity", additionalResource);
        }

        // Ensure that acquiring beyond max capacity will time out or behave properly
        BlockMasterClient timeoutResource = clientPool.acquire(10, TimeUnit.MILLISECONDS);
        Assert.assertNull("Resource acquisition should timeout when max capacity is reached", timeoutResource);

        // 4. Code after testing
        // Release acquired resources back to the pool
        clientPool.release(resource);

        // Proper cleanup to return all resources safely
        for (int i = 0; i < maxCapacity - 1; i++) {
            BlockMasterClient additionalResource = clientPool.acquire(1000, TimeUnit.MILLISECONDS);
            Assert.assertNotNull("Resources acquired should not be null during cleanup", additionalResource);
            clientPool.release(additionalResource);
        }
    }
}