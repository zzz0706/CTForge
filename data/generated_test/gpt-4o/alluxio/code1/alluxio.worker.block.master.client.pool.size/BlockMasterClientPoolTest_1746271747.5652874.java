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
        // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // Obtain the configuration value for max capacity
        int maxCapacity = ServerConfiguration.getInt(PropertyKey.WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE);
        Assert.assertTrue("Max capacity should be greater than 0", maxCapacity > 0);

        // 2. Prepare the test conditions.
        // Create an instance of BlockMasterClientPool using the configuration.
        BlockMasterClientPool clientPool = new BlockMasterClientPool();
        
        // 3. Test code.
        // Test acquiring a resource within the max capacity.
        BlockMasterClient resource = clientPool.acquire(1000, TimeUnit.MILLISECONDS);
        Assert.assertNotNull("A valid resource should be acquired from the pool", resource);

        // Test repeated acquisitions respecting max capacity.
        for (int i = 0; i < maxCapacity - 1; i++) {
            BlockMasterClient additionalResource = clientPool.acquire(1000, TimeUnit.MILLISECONDS);
            Assert.assertNotNull("Resource should still be acquirable before reaching max capacity", additionalResource);
        }

        // Test acquisition beyond max capacity should return null or timeout.
        BlockMasterClient timeoutResource = clientPool.acquire(10, TimeUnit.MILLISECONDS);
        Assert.assertNull("Resource acquisition should timeout when max capacity is reached", timeoutResource);

        // 4. Code after testing.
        // Release all resources back to the pool to ensure proper cleanup.
        clientPool.release(resource);
        for (int i = 0; i < maxCapacity - 1; i++) {
            BlockMasterClient additionalResource = clientPool.acquire(1000, TimeUnit.MILLISECONDS);
            Assert.assertNotNull("Resource acquired for cleanup should not be null", additionalResource);
            clientPool.release(additionalResource);
        }
    }
}