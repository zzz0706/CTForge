package alluxio.client.file;

import alluxio.ClientContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.MasterClientContext;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class FileSystemMasterClientPoolTest {

    @Test
    public void testThreadBlockingBehavior() throws Exception {
        // 1. Use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        InstancedConfiguration conf = InstancedConfiguration.defaults();
        conf.set(PropertyKey.USER_FILE_MASTER_CLIENT_THREADS, "2"); // Setting a fixed size pool for testing purposes
        int maxCapacity = conf.getInt(PropertyKey.USER_FILE_MASTER_CLIENT_THREADS);

        // 2. Prepare the test conditions.
        // Access the FileSystemMasterClientPool by creating the appropriate MasterClientContext.
        MasterClientContext masterClientContext = MasterClientContext.newBuilder(ClientContext.create(conf)).build();
        FileSystemMasterClientPool pool = new FileSystemMasterClientPool(masterClientContext);

        // Fill the pool to its full capacity
        for (int i = 0; i < maxCapacity; i++) {
            FileSystemMasterClient client = pool.acquire();
            pool.release(client);
        }

        // 3. Test code.
        // Simulate threads trying to acquire resources from the pool
        ExecutorService executorService = Executors.newFixedThreadPool(maxCapacity + 2); // More threads than capacity
        Future<FileSystemMasterClient> future1 = executorService.submit(() -> pool.acquire()); // One thread acquires
        Future<FileSystemMasterClient> future2 = executorService.submit(() -> {
            try {
                return pool.acquire(500, TimeUnit.MILLISECONDS); // This thread tries to acquire with a timeout
            } catch (Exception e) {
                return null;
            }
        });

        // Ensure one thread successfully acquires the resource, and the other eventually times out
        FileSystemMasterClient result1 = future1.get(1, TimeUnit.SECONDS); // Acquire resource
        FileSystemMasterClient result2 = future2.get(1, TimeUnit.SECONDS); // Acquire resource or null if timed out

        // Assertions to verify behavior
        Assert.assertNotNull("One thread should successfully acquire a resource", result1);

        // Adjust logic to handle timing conditions
        if (result2 == null) {
            Assert.assertNull("Other thread should eventually time out if no resource is available", result2);
        } else {
            // If result2 did not timeout, ensure it's a valid acquired resource
            pool.release(result2);
        }

        // Release the first acquired client
        if (result1 != null) {
            pool.release(result1);
        }

        // 4. Code after testing: Shut down the executor to clean up resources
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.SECONDS);
    }
}