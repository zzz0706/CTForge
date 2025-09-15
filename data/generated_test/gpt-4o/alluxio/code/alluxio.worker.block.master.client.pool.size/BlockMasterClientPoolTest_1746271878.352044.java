package alluxio.worker.block;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.worker.block.BlockMasterClientPool;
import alluxio.resource.ResourcePool;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class BlockMasterClientPoolTest {
  
    @Test
    public void testAcquireResourceTimeout() throws Exception {
        // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        int maxPoolSize = ServerConfiguration.getInt(PropertyKey.WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE);

        // 2. Prepare the test conditions.
        BlockMasterClientPool clientPool = new BlockMasterClientPool();

        // Fill the pool with resources up to max capacity
        for (int i = 0; i < maxPoolSize; i++) {
            Assert.assertNotNull("Expected to successfully acquire when pool is not full", 
                clientPool.acquire(0, null));  // Acquire resources without timeout
        }

        // 3. Test code: Attempt to acquire another resource with a timeout specified.
        long timeoutMillis = 1000; // Timeout specified in milliseconds
        Object resource = clientPool.acquire(timeoutMillis, TimeUnit.MILLISECONDS);

        // 4. Code after testing: Verify the expected result.
        Assert.assertNull("Expected acquire to return null after timeout expiration when pool is at max capacity", 
            resource);
    }
}