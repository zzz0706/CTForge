package alluxio.worker.block;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.resource.ResourcePool;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class BlockMasterClientPoolTest {

    @Test
    public void testResourcePoolTimeoutWhenNoResourcesAvailable() {
        // 1. Use the Alluxio 2.1.0 API correctly to obtain configuration values.
        int maxCapacity = ServerConfiguration.getInt(PropertyKey.WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE);

        // 2. Prepare the test conditions.
        // Create a ResourcePool instance with maxCapacity set to the configuration value.
        ResourcePool<Object> resourcePool = new ResourcePool<Object>(maxCapacity) {
            @Override
            protected Object createNewResource() {
                // Return a dummy resource for testing.
                return new Object();
            }

            @Override
            public void close() {
                // Override the close method as it's abstract and must be implemented.
                // For this test, we do not need any specific cleanup logic.
            }
        };

        // Acquire resources until the current capacity reaches maxCapacity.
        for (int i = 0; i < maxCapacity; i++) {
            Assert.assertNotNull(resourcePool.acquire(0, null));
        }

        // 3. Test code.
        // Attempt to acquire another resource with a timeout of 500 milliseconds.
        long timeoutMs = 500;
        Object resource = resourcePool.acquire(timeoutMs, TimeUnit.MILLISECONDS);

        // 4. Code after testing.
        // Verify that the acquired resource is null due to timeout when the pool is exhausted.
        Assert.assertNull(resource);
    }
}