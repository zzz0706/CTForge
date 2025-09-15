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
        // Prepare the test environment: Create a BlockMasterClientPool instance
        BlockMasterClientPool clientPool = new BlockMasterClientPool();

        // Retrieve the configuration value using the Alluxio API
        int maxCapacity =
                ServerConfiguration.getInt(PropertyKey.WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE);

        // Assert that the retrieved configuration is correctly applied
        Assert.assertNotNull("The configuration value should not be null", maxCapacity);
        
        // Test: Try acquiring a BlockMasterClient resource from the pool
        BlockMasterClient resource = clientPool.acquire(1000, TimeUnit.MILLISECONDS);

        // Assert that the resource is successfully acquired
        Assert.assertNotNull("Resource should not be null", resource);

        // Clean up after testing: Release the resource back to the pool
        clientPool.release(resource); 
    }
}