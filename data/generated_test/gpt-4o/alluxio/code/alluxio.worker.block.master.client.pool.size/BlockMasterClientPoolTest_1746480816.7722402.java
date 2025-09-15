package alluxio.worker.block;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.resource.ResourcePool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;

public class BlockMasterClientPoolTest {

    private ResourcePool<Object> resourcePool;

    @Before
    public void setUp() {
        // Prepare the test conditions
        // Retrieve the maxCapacity from Alluxio configuration using the correct PropertyKey
        int maxCapacity = ServerConfiguration.getInt(PropertyKey.WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE);

        // Initialize the ResourcePool with the retrieved maxCapacity
        resourcePool = new ResourcePool<Object>(maxCapacity) {
            @Override
            protected Object createNewResource() {
                return new Object();
            }

            @Override
            public void close() {
                // Implementation for the abstract method close
            }
        };
    }

    @Test
    public void testResourceAcquisitionWithNullTimeUnit() {
        // Test code
        // Verify that acquire method handles null TimeUnit correctly for indefinite waiting
        Object acquiredResource = resourcePool.acquire(0, null);

        // Assert that a resource is acquired successfully
        assertNotNull("Resource should be acquired successfully.", acquiredResource);
    }

    @After
    public void tearDown() {
        // Code after testing
        resourcePool = null; // Reset the ResourcePool instance to null
    }
}