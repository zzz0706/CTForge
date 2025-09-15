package alluxio.master.metastore.caching;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.file.meta.InodeLockManager;
import alluxio.master.metastore.InodeStore;
import alluxio.master.metastore.caching.CachingInodeStore;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CachingInodeStoreTest {

    @Test
    public void test_CachingInodeStore_initialization_with_valid_configuration() {
        // 1. Retrieve Alluxio 2.1.0 configuration values using the ServerConfiguration API.
        int maxCacheSize = ServerConfiguration.getInt(PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE);
        float highWaterMarkRatio = ServerConfiguration.getFloat(PropertyKey.MASTER_METASTORE_INODE_CACHE_HIGH_WATER_MARK_RATIO);
        float lowWaterMarkRatio = ServerConfiguration.getFloat(PropertyKey.MASTER_METASTORE_INODE_CACHE_LOW_WATER_MARK_RATIO);

        // Calculate high and low water marks based on the configuration ratios.
        int highWaterMark = Math.round(maxCacheSize * highWaterMarkRatio);
        int lowWaterMark = Math.round(maxCacheSize * lowWaterMarkRatio);

        // 2. Prepare mocked dependencies (InodeStore and InodeLockManager).
        InodeStore backingStoreMock = Mockito.mock(InodeStore.class);
        InodeLockManager lockManagerMock = Mockito.mock(InodeLockManager.class);

        // 3. Initialize CachingInodeStore with the valid arguments.
        CachingInodeStore cachingInodeStore = new CachingInodeStore(
                backingStoreMock,
                lockManagerMock
        );

        // 4. Verify the initialization and configuration values.
        assertNotNull(cachingInodeStore);
        assertTrue(maxCacheSize > 0);
        assertTrue(highWaterMark > 0);
        assertTrue(lowWaterMark > 0);
    }
}