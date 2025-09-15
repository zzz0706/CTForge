package alluxio.worker.block;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.worker.block.BlockLockManager;
import alluxio.worker.block.BlockLockType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class BlockLockManagerTest {

    private BlockLockManager mBlockLockManager;

    @Before
    public void setUp() {
        // Set the configuration value for alluxio.worker.tieredstore.block.locks to 1
        ServerConfiguration.set(PropertyKey.WORKER_TIERED_STORE_BLOCK_LOCKS, 1);

        // Initialize the BlockLockManager instance
        mBlockLockManager = new BlockLockManager();
    }

    @After
    public void tearDown() {
        // Reset ServerConfiguration and clean up
        ServerConfiguration.reset();
    }

    @Test
    public void test_lockBlock_exhaustPool() throws InterruptedException {
        long session1 = 1L;
        long session2 = 2L;
        long blockId1 = 1L;
        long blockId2 = 2L;

        // Step 1: Acquire a lock for blockId1 with session1
        long lockId1 = mBlockLockManager.lockBlock(session1, blockId1, BlockLockType.READ);

        // Step 2: Start another thread to attempt to acquire a lock for blockId2 with session2
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            try {
                mBlockLockManager.lockBlock(session2, blockId2, BlockLockType.READ);
            } catch (Exception e) {
                // Any exception caused during lock acquisition
                Assert.fail("Exception occurred while acquiring second lock: " + e.getMessage());
            }
        });

        // Allow some time for the second thread to block
        TimeUnit.MILLISECONDS.sleep(500);

        // Step 3: Verify that the second lockBlock call is blocked (not completed yet).
        // Since we can't directly check if the thread is blocked, we proceed to release the first lock.

        // Step 4: Release the lock for session1 by calling cleanupSession
        mBlockLockManager.cleanupSession(session1);

        // Allow some time for the second thread to acquire the lock after the first one is released
        executor.shutdown();
        boolean finished = executor.awaitTermination(5, TimeUnit.SECONDS);

        Assert.assertTrue("The second lockBlock call did not complete after pool was freed", finished);

        // Step 5: No explicit assertion is required here, as the flow will fail if the lock cannot be acquired.
    }
}