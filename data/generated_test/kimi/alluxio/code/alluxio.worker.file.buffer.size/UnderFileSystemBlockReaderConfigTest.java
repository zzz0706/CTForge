package alluxio.worker.block;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.worker.block.meta.UnderFileSystemBlockMeta;
import alluxio.worker.block.BlockStore;
import alluxio.underfs.UfsManager;
import alluxio.worker.block.UnderFileSystemBlockReader;
import alluxio.worker.block.UfsInputStreamManager;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServerConfiguration.class})
public class UnderFileSystemBlockReaderConfigTest {

    @Before
    public void setUp() {
        PowerMockito.mockStatic(ServerConfiguration.class);
        PowerMockito.when(ServerConfiguration.getBytes(PropertyKey.WORKER_FILE_BUFFER_SIZE)).thenReturn(-1024L);
    }

    @After
    public void tearDown() {
        PowerMockito.mockStatic(ServerConfiguration.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeValueIsRejected() throws Exception {
        UnderFileSystemBlockMeta mockMeta = PowerMockito.mock(UnderFileSystemBlockMeta.class);
        BlockStore mockStore = PowerMockito.mock(BlockStore.class);
        UfsManager mockUfsManager = PowerMockito.mock(UfsManager.class);
        UfsInputStreamManager mockUfsInStreamManager = PowerMockito.mock(UfsInputStreamManager.class);

        UnderFileSystemBlockReader.create(
            mockMeta,
            0L,
            mockStore,
            mockUfsManager,
            mockUfsInStreamManager
        );
    }
}