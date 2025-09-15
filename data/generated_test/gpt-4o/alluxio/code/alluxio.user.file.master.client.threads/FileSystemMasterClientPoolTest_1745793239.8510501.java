package alluxio.client.file;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.resource.ResourcePool;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class FileSystemMasterClientPoolTest {

    @Test
    public void testResourcePoolBlockingBehaviorWhenAtCapacity() throws InterruptedException {
        // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        AlluxioProperties properties = new AlluxioProperties();
        properties.set(PropertyKey.USER_FILE_MASTER_CLIENT_THREADS, "2");
        AlluxioConfiguration conf = new InstancedConfiguration(properties);

        // Extracting the configuration value dynamically.
        int maxCapacity = conf.getInt(PropertyKey.USER_FILE_MASTER_CLIENT_THREADS);

        // 2. Prepare the test conditions.
        ResourcePool<Object> resourcePool = new ResourcePool<Object>(maxCapacity) {
            @Override
            protected Object createNewResource() {
                return new Object(); // Simulating resource creation.
            }

            @Override
            public void close() {
                // Implementing close as required by the abstract class.
            }
        };

        // Fill ResourcePool to its maximum capacity.
        for (int i = 0; i < maxCapacity; i++) {
            Assert.assertNotNull(resourcePool.acquire(0, null)); // Non-blocking acquire.
        }

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);

        // 3. Test code.
        try {
            executor.submit(() -> {
                try {
                    // Attempt to acquire with a timeout, should block until a resource becomes available.
                    Object resource = resourcePool.acquire(5000, TimeUnit.MILLISECONDS);
                    Assert.assertNotNull("Blocked acquire should return a valid resource after release.", resource);
                } catch (Exception e) {
                    Assert.fail("Unexpected exception during resource acquisition: " + e.getMessage());
                }
            });

            // Introduce a small delay to ensure the blocking acquire operation is initiated.
            Thread.sleep(1000);

            // Release one resource back into the pool.
            resourcePool.release(new Object());

            // Wait for the executor's task to complete and verify blocking acquire behavior.
            executor.shutdown();
            boolean finished = executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            Assert.assertTrue("The blocking acquire operation did not complete within the expected time.", finished);
        } finally {
            // 4. Code after testing.
            executor.shutdownNow();
        }
    }
}