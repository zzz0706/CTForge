package alluxio.client.file;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.resource.ResourcePool;
import alluxio.conf.AlluxioProperties;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class FileSystemMasterClientPoolTest {
    private static final long TEST_TIMEOUT_MS = 5000;

    @Test
    public void testResourcePoolBlockingBehaviorWhenAtCapacity() throws InterruptedException {
        // 1. Using the Alluxio 2.1.0 API correctly to obtain configuration values.
        AlluxioProperties properties = new AlluxioProperties();
        AlluxioConfiguration conf = new InstancedConfiguration(properties);
        int maxCapacity = conf.getInt(PropertyKey.USER_FILE_MASTER_CLIENT_THREADS);

        // 2. Prepare a ResourcePool with the configured max capacity.
        ResourcePool<Object> resourcePool = new ResourcePool<Object>(maxCapacity) {
            @Override
            protected Object createNewResource() {
                return new Object();
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
        try {
            // 3. Test code: Attempt to acquire a resource with blocking — this should block as pool is at capacity.
            executor.submit(() -> {
                try {
                    // This should block until a resource becomes available.
                    Object resource = resourcePool.acquire(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    Assert.assertNotNull("Blocked acquire should return a valid resource after release.", resource);
                } catch (Exception e) {
                    Assert.fail("Unexpected exception during resource acquisition: " + e.getMessage());
                }
            });

            // Simulate a small delay to ensure the blocking acquire has time to execute.
            Thread.sleep(1000);

            // Release one resource back into the pool.
            resourcePool.release(new Object());

            // Wait for the executor's task to finish and verify blocking behavior.
            executor.shutdown();
            boolean finished = executor.awaitTermination(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            Assert.assertTrue("The blocking acquire did not complete within the expected time.", finished);
        } finally {
            executor.shutdownNow();
        }
    }
}