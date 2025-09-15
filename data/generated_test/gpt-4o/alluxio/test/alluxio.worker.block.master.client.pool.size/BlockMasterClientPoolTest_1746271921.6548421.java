package alluxio.worker.block;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.worker.block.BlockMasterClientPool;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Unit tests for BlockMasterClientPool functionality.
 */
public class BlockMasterClientPoolTest {

    @Test
    public void testAcquireResourceTimeout() throws Exception {
        // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        int maxPoolSize = ServerConfiguration.getInt(PropertyKey.WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE);
        long timeoutMillis = 1000; // Timeout value for the acquire method.

        // 2. Prepare the test conditions.
        // Initialize BlockMasterClientPool.
        BlockMasterClientPool clientPool = new BlockMasterClientPool();

        // Acquire resources up to max capacity and ensure the pool respects max capacity.
        for (int i = 0; i < maxPoolSize; i++) {
            Assert.assertNotNull("Expected to successfully acquire when the pool is not full", 
                clientPool.acquire(0, null)); // Acquire resources without timeout.
        }

        // 3. Test code: Attempt to acquire another resource with a timeout specified.
        Object resource = clientPool.acquire(timeoutMillis, TimeUnit.MILLISECONDS);

        // 4. Code after testing: Ensure acquire returns null after timeout expiration when pool is at max capacity.
        Assert.assertNull("Expected acquire to return null after timeout expiration when pool is at max capacity", 
            resource);
    }
}