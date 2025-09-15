package alluxio.master.metastore.caching;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.file.meta.InodeLockManager;
import alluxio.master.metastore.InodeStore;
import org.junit.Test;
import org.mockito.Mockito;

public class CachingInodeStoreTest {
    @Test(expected = IllegalStateException.class)
    public void test_CachingInodeStore_constructor_with_invalid_configuration() {
        // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        ServerConfiguration.set(PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE, "-1");
        ServerConfiguration.set(PropertyKey.MASTER_METASTORE_INODE_CACHE_HIGH_WATER_MARK_RATIO, "0.5");
        ServerConfiguration.set(PropertyKey.MASTER_METASTORE_INODE_CACHE_LOW_WATER_MARK_RATIO, "0.6");

        // 2. Prepare the test conditions: Use Mockito to mock the InodeStore and InodeLockManager interfaces.
        InodeStore inodeStore = Mockito.mock(InodeStore.class);
        InodeLockManager inodeLockManager = Mockito.mock(InodeLockManager.class);

        // 3. Test code: Construct CachingInodeStore with invalid configuration, expecting an IllegalStateException.
        new CachingInodeStore(inodeStore, inodeLockManager);
    }
}