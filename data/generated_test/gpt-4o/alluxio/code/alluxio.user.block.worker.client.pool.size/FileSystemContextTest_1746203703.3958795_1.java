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

    @Test
    public void testAcquireBlockWorkerClientWithMultipleConcurrentRequests() throws Exception {
        // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // Prepare the configuration and dynamically fetch the client pool size.
        AlluxioConfiguration conf = InstancedConfiguration.defaults();
        int poolSize = conf.getInt(PropertyKey.USER_BLOCK_WORKER_CLIENT_POOL_SIZE);

        // 2. Prepare the test conditions.
        // Create a WorkerNetAddress for testing.
        WorkerNetAddress workerNetAddress = new WorkerNetAddress()
                .setHost("localhost")
                .setRpcPort(8080)
                .setDataPort(8081)
                .setWebPort(8082)
                .setDomainSocketPath("");

        // Create a FileSystemContext instance using the configuration.
        FileSystemContext fileSystemContext = FileSystemContext.create(conf);

        // Create a thread pool with more threads than the pool size for concurrent testing.
        ExecutorService executorService = Executors.newFixedThreadPool(poolSize + 5);

        // 3. Test code.
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

        // 4. Code after testing.
        // Shut down the thread pool and wait for termination.
        executorService.shutdown();
        boolean completed = executorService.awaitTermination(30, TimeUnit.SECONDS);
        if (!completed) {
            throw new RuntimeException("Test threads did not complete within the timeout period.");
        }
    }
}