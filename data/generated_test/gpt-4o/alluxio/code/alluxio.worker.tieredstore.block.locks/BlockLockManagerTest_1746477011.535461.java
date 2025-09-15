package alluxio.worker.block;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.worker.block.BlockLockManager;
import alluxio.worker.block.BlockLockType;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class BlockLockManagerTest {

    private BlockLockManager mBlockLockManager;

    @Before
    public void setup() throws Exception {
        // Prepare the test conditions
        // Correctly use Alluxio 2.1.0 API to set the configuration value for alluxio.worker.tieredstore.block.locks
        ServerConfiguration.set(PropertyKey.WORKER_TIERED_STORE_BLOCK_LOCKS, "10");
        mBlockLockManager = new BlockLockManager();
    }

    @Test
    public void test_lockBlock_addReference_existingLock() throws Exception {
        // Test code
        long sessionId = 1L;
        long blockId = 12345L;

        // Acquire a read lock twice for the same block ID
        long lockId1 = mBlockLockManager.lockBlock(sessionId, blockId, BlockLockType.READ);
        long lockId2 = mBlockLockManager.lockBlock(sessionId, blockId, BlockLockType.READ);
        
        // Assert that lock IDs are different, indicating separate lock calls
        assertTrue(lockId1 != lockId2);

        // Unlock the locks and release resources
        mBlockLockManager.cleanupSession(sessionId);
    }
}