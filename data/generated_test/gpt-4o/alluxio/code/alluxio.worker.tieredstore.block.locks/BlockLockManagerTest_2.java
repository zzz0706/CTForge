package alluxio.worker.block;

import alluxio.conf.ServerConfiguration;
import alluxio.worker.block.BlockLockManager;
import alluxio.worker.block.BlockLockType;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class BlockLockManagerTest {

    private BlockLockManager mBlockLockManager;

    @Before
    public void setUp() {
        // Initialize the BlockLockManager instance using the server configuration.
        mBlockLockManager = new BlockLockManager();
    }

    @Test
    public void test_lockBlock_readLock_acquisition() {
        // Test conditions
        long sessionId = 1L; // Sample session ID
        long blockId = 123L; // Sample block ID to acquire a lock for
        
        // Perform operation: Acquire a read lock for the block ID
        long lockId = mBlockLockManager.lockBlock(sessionId, blockId, BlockLockType.READ);
        
        // Verify: Ensure the lock acquisition returned a valid lock ID
        assertNotNull("The returned lock ID should not be null", lockId);

        // Cleanup after test
        mBlockLockManager.cleanupSession(sessionId);
    }
}