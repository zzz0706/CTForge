package alluxio.worker.block;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.worker.block.BlockMasterClient;
import alluxio.worker.block.BlockMasterClientPool;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Unit test for BlockMasterClientPool to ensure configuration usage is tested and code coverage is maximized.
 */
public class BlockMasterClientPoolTest {

    @Test
    public void testBlockMasterClientPoolAcquireConfigurationUsage() {
        // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // Obtain the configuration value for max capacity dynamically from the Alluxio configuration system.
        int maxCapacity = ServerConfiguration.getInt(PropertyKey.WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE);
        Assert.assertTrue("Max capacity should be greater than 0", maxCapacity > 0);

        // 2. Prepare the test conditions.
        // Create an instance of BlockMasterClientPool using the configuration value for max capacity.
        BlockMasterClientPool clientPool = new BlockMasterClientPool();

        // 3. Test code.
        // Test acquiring a resource from the pool within the configured max capacity.
        BlockMasterClient resource = clientPool.acquire(1000, TimeUnit.MILLISECONDS);
        Assert.assertNotNull("A valid resource should be acquired from the pool", resource);

        // Validate repeated acquisitions respecting max capacity.
        for (int i = 0; i < maxCapacity - 1; i++) {
            BlockMasterClient additionalResource = clientPool.acquire(1000, TimeUnit.MILLISECONDS);
            Assert.assertNotNull("Should be able to acquire resources up to the max capacity", additionalResource);
        }

        // Test acquisition beyond max capacity to ensure proper blocking or timeout behavior when resources are unavailable.
        BlockMasterClient timeoutResource = clientPool.acquire(10, TimeUnit.MILLISECONDS);
        Assert.assertNull("Resource acquisition should timeout when max capacity is reached", timeoutResource);

        // 4. Code after testing.
        // Release all resources back to the pool to ensure proper cleanup and consistency.
        clientPool.release(resource);
        for (int i = 0; i < maxCapacity - 1; i++) {
            BlockMasterClient additionalResource = clientPool.acquire(1000, TimeUnit.MILLISECONDS);
            Assert.assertNotNull("Resource acquired for cleanup should not be null", additionalResource);
            clientPool.release(additionalResource);
        }
    }
}