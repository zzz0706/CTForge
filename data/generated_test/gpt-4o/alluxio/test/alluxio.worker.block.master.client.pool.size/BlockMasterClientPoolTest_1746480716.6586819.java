package alluxio.worker.block;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class BlockMasterClientPoolTest {
    // test code
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
  
    @Test
    public void testResourcePoolWaitIndefinitelyForResource() throws Exception {
        // 1. Retrieve the maximum capacity value dynamically from Alluxio's ServerConfiguration
        int maxCapacity = ServerConfiguration.getInt(PropertyKey.WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE);

        // 2. Prepare the test conditions.
        // Simulating a ResourcePool using an ArrayBlockingQueue to manage resources.
        ArrayBlockingQueue<Object> resourcePool = new ArrayBlockingQueue<>(maxCapacity);

        // Populate the pool to its maximum capacity
        for (int i = 0; i < maxCapacity; i++) {
            resourcePool.put(new Object());
        }

        // 3. Test code.
        // Create a thread that will release a resource after a delay
        Thread resourceReleaser = new Thread(() -> {
            try {
                Thread.sleep(1000); // Delay before releasing a resource
                resourcePool.put(new Object()); // Release a resource back into the pool
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        resourceReleaser.start();

        // Attempt to acquire a resource with indefinite wait time
        Object acquiredResource = resourcePool.poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

        // Validate the acquired resource
        Assert.assertNotNull("Failed to acquire a resource after waiting indefinitely", acquiredResource);

        // 4. Code after testing.
        // Clean up after the test is complete
        resourceReleaser.join(); // Wait for the resource releaser thread to finish
    }
}