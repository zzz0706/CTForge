package alluxio.worker.block;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.worker.block.BlockMasterClientPool;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class BlockMasterClientPoolTest {
    @Test
    public void testAcquireResourceTimeout() throws Exception {
        // 1. Use Alluxio 2.1.0 API to obtain configuration values
        int poolSize = ServerConfiguration.getInt(PropertyKey.WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE);

        // 2. Prepare the test conditions
        BlockMasterClientPool clientPool = new BlockMasterClientPool();

        // Fill the pool with resources up to max capacity
        for (int i = 0; i < poolSize; i++) {
            clientPool.acquire(0, null);  // Acquire resources without timeout
        }

        // 3. Test code: Attempt to acquire another resource with a timeout specified
        long timeout = 1000;  // Timeout in milliseconds
        Object resource = clientPool.acquire(timeout, TimeUnit.MILLISECONDS);

        // 4. Code after testing: Verify the expected result
        // The acquire method should return null after the timeout expires
        Assert.assertNull("Expected acquire to return null after timeout expiration", resource);
    }
}