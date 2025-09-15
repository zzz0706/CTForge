package alluxio.worker.block;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.worker.block.BlockLockManager;
import alluxio.worker.block.BlockLockType;
import org.junit.Assert;
import org.junit.Test;

public class BlockLockManagerTest {
    /**
     * Test case: Ensure proper handling when the lock pool is exhausted, and validate blocking behavior.
     * Objective: Validate that the acquire method blocks temporarily while waiting for an available lock,
     * but eventually succeeds once a lock is released back to the pool.
     */
    @Test
    public void test_lockBlock_pool_exhaustion_handling_on_acquire() throws InterruptedException {
        // Step 1: Obtain configuration value using Alluxio API
        int maxBlockLocks = ServerConfiguration.getInt(PropertyKey.WORKER_TIERED_STORE_BLOCK_LOCKS);

        // Step 2: Initialize the BlockLockManager and prepare test conditions
        BlockLockManager lockManager = new BlockLockManager();

        long sessionId = 1L;
        long blockId1 = 100L;
        long blockId2 = 101L;

        // Step 3: Acquire locks up to the configured maximum
        lockManager.lockBlock(sessionId, blockId1, BlockLockType.WRITE);
        lockManager.lockBlock(sessionId, blockId2, BlockLockType.WRITE);

        // Step 4: Attempt to acquire another lock for a new blockId while pool is fully utilized
        Thread acquisitionThread = new Thread(() -> {
            try {
                long blockId3 = 102L;
                lockManager.lockBlock(sessionId, blockId3, BlockLockType.WRITE);
                Assert.assertTrue("Successfully acquired lock for blockId3 after pool exhaustion", true);
            } catch (Exception e) {
                Assert.fail("Failed to handle lock acquisition due to pool exhaustion: " + e.getMessage());
            }
        });

        acquisitionThread.start();

        // Step 5: Simulate lock release to allow new acquisition
        Thread.sleep(500); // Wait for the acquisition thread to block
        lockManager.cleanupSession(sessionId);

        acquisitionThread.join(); // Ensure acquisition thread ends successfully
    }
}