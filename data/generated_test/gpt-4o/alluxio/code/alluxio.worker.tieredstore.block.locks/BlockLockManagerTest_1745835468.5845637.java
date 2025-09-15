package alluxio.worker.block;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.worker.block.BlockLockManager;
import alluxio.worker.block.BlockLockType;
import org.junit.Assert;
import org.junit.Test;

public class BlockLockManagerTest {
  
    @Test
    public void test_lockBlock_pool_exhaustion_handling_on_acquire() throws InterruptedException {
        // 1. Use Alluxio 2.1.0 API to obtain configuration values dynamically
        int maxBlockLocks = ServerConfiguration.getInt(PropertyKey.WORKER_TIERED_STORE_BLOCK_LOCKS);

        // 2. Prepare test conditions: Initialize BlockLockManager
        BlockLockManager lockManager = new BlockLockManager();

        long sessionId = 1L;
        long blockId1 = 100L;
        long blockId2 = 101L;

        // Acquire locks up to the configured maximum
        lockManager.lockBlock(sessionId, blockId1, BlockLockType.WRITE);
        lockManager.lockBlock(sessionId, blockId2, BlockLockType.WRITE);

        // 3. Test code: Attempt to acquire another lock while the pool is fully utilized
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

        // Simulate lock release to allow new acquisition
        Thread.sleep(500); // Wait for the acquisition thread to block
        lockManager.cleanupSession(sessionId); // Release locks held by the session

        acquisitionThread.join(); // Ensure acquisition thread completes successfully
    }
}