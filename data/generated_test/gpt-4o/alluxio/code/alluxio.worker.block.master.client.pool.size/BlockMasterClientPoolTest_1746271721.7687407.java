package alluxio.worker.block;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.worker.block.BlockMasterClient;
import alluxio.worker.block.BlockMasterClientPool;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class BlockMasterClientPoolTest {

    /**
     * Test to verify that the acquire method successfully returns a resource when
     * the max capacity is not reached and configuration usage is covered.
     */
    @Test
    public void testBlockMasterClientPoolAcquireResourceSuccess() {
        // Step 1: Retrieve the configuration value for max pool capacity using Alluxio 2.1.0 API
        int maxCapacity = ServerConfiguration.getInt(PropertyKey.WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE);
        Assert.assertTrue("Max capacity should be greater than 0", maxCapacity > 0);

        // Step 2: Create a BlockMasterClientPool instance
        BlockMasterClientPool clientPool = new BlockMasterClientPool();

        // Step 3: Try acquiring a resource from the pool
        BlockMasterClient resource = clientPool.acquire(1000, TimeUnit.MILLISECONDS);

        // Step 4: Assert that the resource was successfully acquired
        Assert.assertNotNull("A valid resource should be acquired from the pool", resource);

        // Step 5: Clean up - release the resource back to the pool
        clientPool.release(resource);
    }
}