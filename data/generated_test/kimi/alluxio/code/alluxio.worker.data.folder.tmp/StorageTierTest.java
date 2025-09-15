package alluxio.worker.block.meta;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import static org.powermock.api.mockito.PowerMockito.when;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.util.io.FileUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

/**
 * Unit tests for {@link StorageTier}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileUtils.class, StorageDir.class})
public class StorageTierTest {

  @Before
  public void setUp() {
    // Ensure ServerConfiguration uses defaults (no explicit set calls)
  }

  @After
  public void tearDown() {
    ServerConfiguration.reset();
  }

  @Test
  public void initStorageTierHandlesIOExceptionOnDeleteGracefully() throws Exception {
    // 1. Configuration as Input – rely on default value for WORKER_DATA_TMP_FOLDER
    String expectedTmpDir = ServerConfiguration.get(PropertyKey.WORKER_DATA_TMP_FOLDER);

    // 2. Mock/Stub external dependencies
    mockStatic(FileUtils.class);
    mockStatic(StorageDir.class);

    // Stub FileUtils.deletePathRecursively to throw IOException
    String anyTmpPath = "/mnt/mem/0/" + expectedTmpDir;
    doThrow(new IOException("mocked io exception"))
        .when(FileUtils.class, "deletePathRecursively", anyString());

    // Stub FileUtils.exists to return true
    when(FileUtils.exists(anyString())).thenReturn(true);

    // Stub StorageDir.newStorageDir to succeed
    StorageDir mockDir = mock(StorageDir.class);
    when(StorageDir.newStorageDir(any(StorageTier.class), anyInt(), anyLong(), anyString(),
        anyString())).thenReturn(mockDir);

    // 3. Invoke the method under test
    StorageTier tier = StorageTier.newStorageTier("MEM");

    // 4. Assertions and verification
    assertNotNull(tier); // tier initialization completes successfully
    verifyStatic();
    FileUtils.deletePathRecursively(anyString());
  }
}