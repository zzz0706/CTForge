package alluxio.client.file;

import alluxio.client.file.FileSystemContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.wire.WorkerNetAddress;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class FileSystemContextTest {

    /**
     * Test case name: testAcquireBlockWorkerClientWithMultipleConcurrentRequests
     * Objective: Verify that the acquireBlockWorkerClient method properly handles multiple concurrent client acquisition requests.
     */
    @Test
    public void testAcquireBlockWorkerClientWithMultipleConcurrentRequests() throws Exception {
        // Step 1: Prepare test conditions.
        // Create an actual WorkerNetAddress instance instead of mocking, as the class is final.
        WorkerNetAddress workerNetAddress = new WorkerNetAddress()
                .setHost("localhost")
                .setRpcPort(8080)
                .setDataPort(8081)
                .setWebPort(8082)
                .setDomainSocketPath("");

        // Create an AlluxioConfiguration instance for testing.
        AlluxioConfiguration conf = InstancedConfiguration.defaults();

        // Create a FileSystemContext instance using the configuration.
        FileSystemContext fileSystemContext = FileSystemContext.create(conf);

        // Dynamically fetch the configuration value using PropertyKey.
        int poolSize = conf.getInt(PropertyKey.USER_BLOCK_WORKER_CLIENT_POOL_SIZE);

        // Create a thread pool with more threads than the pool size for concurrent testing.
        ExecutorService executorService = Executors.newFixedThreadPool(poolSize + 5);

        // Step 2: Test code.
        for (int i = 0; i < poolSize + 5; i++) {
            executorService.submit(() -> {
                try {
                    // Acquire and close a block worker client with the WorkerNetAddress.
                    fileSystemContext.acquireBlockWorkerClient(workerNetAddress).close();
                } catch (Exception e) {
                    // Print exceptions during client acquisition for debugging.
                    e.printStackTrace();
                }
            });
        }

        // Step 3: Code after testing.
        // Shut down the thread pool and wait for termination.
        executorService.shutdown();
        boolean completed = executorService.awaitTermination(30, TimeUnit.SECONDS);
        if (!completed) {
            throw new RuntimeException("Test threads did not complete within the timeout period.");
        }

        // Additional assertions could be added if necessary (e.g., verifying internal state).
    }
}