package alluxio.worker.block;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.worker.block.BlockLockManager;
import alluxio.worker.block.BlockLockType;
import org.junit.Test;

import static org.junit.Assert.fail;

public class BlockLockManagerTest {

    @Test
    //test code
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_lockBlock_writeLock_acquisition_failure_due_to_double_locking() {
        // Step 1: Obtain the configuration value using the Alluxio 2.1.0 API.
        int blockLocksConfigValue = ServerConfiguration.getInt(PropertyKey.WORKER_TIERED_STORE_BLOCK_LOCKS);

        // Step 2: Prepare the test conditions by initializing BlockLockManager using the configuration value.
        BlockLockManager blockLockManager = new BlockLockManager();

        // Define test input: session ID and block ID.
        long sessionId = 1L; // Sample session ID
        long blockId = 100L; // Sample block ID

        // Step 3: Write test code to verify expected behavior.
        try {
            // Acquiring a write lock for the given block ID and session ID.
            blockLockManager.lockBlock(sessionId, blockId, BlockLockType.WRITE);

            // Attempting to acquire another write lock on the same block ID for the same session ID.
            blockLockManager.lockBlock(sessionId, blockId, BlockLockType.WRITE);

            // If no exception is thrown, fail the test since double write locks are not allowed.
            fail("Expected an IllegalStateException to be thrown");
        } catch (IllegalStateException e) {
            // Expected exception ensures the method behaves correctly.
            // Test passed.
        } finally {
            // Step 4: Clean up after testing to release any held locks.
            blockLockManager.cleanupSession(sessionId);
        }
    }
}