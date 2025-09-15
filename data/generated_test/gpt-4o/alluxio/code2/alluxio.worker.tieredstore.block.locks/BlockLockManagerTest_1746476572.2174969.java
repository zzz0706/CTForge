package alluxio.worker.block;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.worker.block.BlockLockManager;
import alluxio.worker.block.BlockLockType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class BlockLockManagerTest {

    private BlockLockManager mBlockLockManager;

    @Before
    public void setUp() {
        // 1. Use the Alluxio 2.1.0 API to configure the system properties.
        ServerConfiguration.set(PropertyKey.WORKER_TIERED_STORE_BLOCK_LOCKS, 1);

        // 2. Initialize the BlockLockManager instance.
        mBlockLockManager = new BlockLockManager();
    }

    @After
    public void tearDown() {
        // 4. Reset the ServerConfiguration and release resources post-testing.
        ServerConfiguration.reset();
    }

    @Test
    public void test_lockBlock_exhaustPool() throws InterruptedException {
        long session1 = 1L;
        long session2 = 2L;
        long blockId1 = 1L;
        long blockId2 = 2L;

        // 3. Test code:
        // Step 1: Acquire a lock for blockId1 with session1 using lockBlock.
        long lockId1 = mBlockLockManager.lockBlock(session1, blockId1, BlockLockType.READ);

        // Step 2: Start another thread to attempt to acquire a lock for blockId2 with session2.
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            // Attempt to acquire lock for blockId2 and handle any exceptions during lock acquisition.
            try {
                mBlockLockManager.lockBlock(session2, blockId2, BlockLockType.READ);
            } catch (Exception e) {
                Assert.fail("Exception occurred while acquiring second lock: " + e.getMessage());
            }
        });

        // Allow some time for the second thread to possibly block due to exhausted pool.
        TimeUnit.MILLISECONDS.sleep(500);

        // Step 3: Simulate releasing the lock for session1 by calling cleanupSession.
        mBlockLockManager.cleanupSession(session1);

        // Wait for the second thread to successfully acquire its lock after the pool is freed.
        executor.shutdown();
        boolean finished = executor.awaitTermination(5, TimeUnit.SECONDS);

        // Validate that the second lockBlock call eventually succeeds.
        Assert.assertTrue("The second lockBlock call did not complete after pool was freed", finished);
    }
}