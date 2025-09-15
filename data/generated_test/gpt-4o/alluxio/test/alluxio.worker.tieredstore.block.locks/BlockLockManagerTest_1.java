package alluxio.worker.block;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.worker.block.BlockLockManager;
import alluxio.worker.block.BlockLockType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class BlockLockManagerTest {

    @Test
    public void test_lockBlock_withWriteLock_successful() throws Exception {
        // 1. Use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        ServerConfiguration.set(PropertyKey.WORKER_TIERED_STORE_BLOCK_LOCKS, "3");
        BlockLockManager blockLockManager = new BlockLockManager();

        // 2. Prepare the test conditions (Test data).
        long sessionId = 1L;
        long blockId = 42L;

        // 3. Test code: Call lockBlock and validate the outcome.
        long lockId = blockLockManager.lockBlock(sessionId, blockId, BlockLockType.WRITE);

        // Access and validate the locked blocks by using the correct public method.
        try {
            Set<Long> sessionBlocks = blockLockManager.getLockedBlocks();
            Assert.assertTrue(sessionBlocks.contains(blockId));
        } finally {
            // 4. Cleanup and verification after testing.
            blockLockManager.cleanupSession(sessionId);
            
            Set<Long> sessionBlocksAfterCleanup = blockLockManager.getLockedBlocks();
            Assert.assertFalse(sessionBlocksAfterCleanup.contains(blockId));
        }
    }
}