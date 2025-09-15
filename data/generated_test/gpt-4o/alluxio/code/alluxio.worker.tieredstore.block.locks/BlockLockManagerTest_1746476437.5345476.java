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
        // Set the configuration 'alluxio.worker.tieredstore.block.locks' to a valid positive value.
        ServerConfiguration.set(PropertyKey.WORKER_TIERED_STORE_BLOCK_LOCKS, "3");

        // Initialize BlockLockManager instance.
        BlockLockManager blockLockManager = new BlockLockManager();

        // 2. Prepare the test conditions (Test data).
        long sessionId = 1L;
        long blockId = 42L;

        // 3. Test code: Call lockBlock and validate the outcome.
        long lockId = blockLockManager.lockBlock(sessionId, blockId, BlockLockType.WRITE);

        try {
            // Validate that the block ID is now in the set of locked blocks.
            Set<Long> sessionBlocks = blockLockManager.getLockedBlocks();
            Assert.assertTrue(
                    "The block lock should be held after lockBlock is called.",
                    sessionBlocks.contains(blockId)
            );

            // Additional validation of write-lock behavior (if necessary).
        } finally {
            // 4. Cleanup and verification after testing.
            blockLockManager.cleanupSession(sessionId);

            Set<Long> sessionBlocksAfterCleanup = blockLockManager.getLockedBlocks();
            Assert.assertFalse(
                    "The block lock should be released after cleanupSession is called.",
                    sessionBlocksAfterCleanup.contains(blockId)
            );
        }
    }
}