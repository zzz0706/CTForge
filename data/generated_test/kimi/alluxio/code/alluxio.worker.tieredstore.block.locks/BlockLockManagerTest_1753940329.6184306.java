package alluxio.worker.block;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.resource.ResourcePool;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.*;

import java.lang.reflect.Field;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServerConfiguration.class})
public class BlockLockManagerTest {

    @Test
    public void verifyLockReleaseReturnsResourceToPool() throws Exception {
        // 1. Use Alluxio 2.1.0 API to obtain the configured value
        PowerMockito.mockStatic(ServerConfiguration.class);
        when(ServerConfiguration.getInt(PropertyKey.WORKER_TIERED_STORE_BLOCK_LOCKS))
            .thenReturn(1);

        // 2. Prepare the test conditions
        BlockLockManager manager = new BlockLockManager();
        Field poolField = BlockLockManager.class.getDeclaredField("mLockPool");
        poolField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ResourcePool<ClientRWLock> originalPool = (ResourcePool<ClientRWLock>) poolField.get(manager);
        ResourcePool<ClientRWLock> spyPool = spy(originalPool);
        poolField.set(manager, spyPool);

        // 3. Test code
        long blockId = 1L;
        long sessionId = 42L;
        long lockId = manager.lockBlock(sessionId, blockId, BlockLockType.WRITE);

        // Verify pool.release was never called before unlock
        verify(spyPool, never()).release(any(ClientRWLock.class));

        manager.unlockBlock(lockId);

        // 4. Code after testing
        verify(spyPool, times(1)).release(any(ClientRWLock.class));
    }
}