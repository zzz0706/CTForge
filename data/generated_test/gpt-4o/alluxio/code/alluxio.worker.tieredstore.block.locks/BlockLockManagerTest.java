package alluxio.worker.block;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.worker.block.BlockLockManager;
import alluxio.worker.block.BlockLockType;
import org.junit.Test;

import static org.junit.Assert.fail;

public class BlockLockManagerTest {

    @Test
    public void test_lockBlock_writeLock_acquisition_failure_due_to_double_locking() {
        // Prepare the test conditions
        // 1. Obtain the configuration value using the Alluxio 2.1.0 API correctly.
        int blockLocksConfigValue = ServerConfiguration.getInt(PropertyKey.WORKER_TIERED_STORE_BLOCK_LOCKS);

        // 2. Initialize BlockLockManager using the configuration value.
        BlockLockManager blockLockManager = new BlockLockManager();

        // Test code
        long sessionId = 1L; // Sample session ID
        long blockId = 100L; // Sample block ID

        try {
            // Acquire a write lock for the given sessionId and blockId
            blockLockManager.lockBlock(sessionId, blockId, BlockLockType.WRITE);

            // Attempt to acquire another write lock on the same blockId for the same sessionId
            blockLockManager.lockBlock(sessionId, blockId, BlockLockType.WRITE);

            // If no exception is thrown, fail the test
            fail("Expected an IllegalStateException to be thrown");
        } catch (IllegalStateException e) {
            // Expected exception occurred, the test passed
            // No further action needed
        } finally {
            // Code after testing: Clean up the session locks
            blockLockManager.cleanupSession(sessionId);
        }
    }
}