package alluxio.worker.block.meta;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.util.io.FileUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

/**
 * Unit test to ensure that {@link StorageDir} uses the configured POSIX permissions when
 * creating storage directories.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileUtils.class})
public class StorageDirInitializeMetaUsesConfiguredPermissionTest {

  @Rule
  public TemporaryFolder mTempFolder = new TemporaryFolder();

  private String mTempDirPath;

  @Before
  public void before() throws Exception {
    mTempDirPath = mTempFolder.newFolder().getAbsolutePath();
    ServerConfiguration.reset();
  }

  @After
  public void after() {
    ServerConfiguration.reset();
  }

  @Test
  public void storageDirInitializeMetaUsesConfiguredPermission() throws Exception {
    // 1. Use the Alluxio 2.1.0 API to set the configuration value instead of hard-coding.
    ServerConfiguration.set(PropertyKey.WORKER_DATA_FOLDER_PERMISSIONS, "rwx------");

    // 2. Prepare the test conditions.
    String expectedPermissions = ServerConfiguration.get(PropertyKey.WORKER_DATA_FOLDER_PERMISSIONS);

    mockStatic(FileUtils.class);
    PowerMockito.when(FileUtils.createStorageDirPath(anyString(), anyString())).thenReturn(true);

    // 3. Test code: trigger StorageDir creation which internally calls initializeMeta().
    StorageDir.newStorageDir(
        StorageTier.newStorageTier("MEM"), 0, 1000, mTempDirPath, "MEM");

    // 4. Code after testing: verify the permission argument passed to FileUtils.
    ArgumentCaptor<String> permissionCaptor = ArgumentCaptor.forClass(String.class);
    verifyStatic();
    FileUtils.createStorageDirPath(eq(mTempDirPath), permissionCaptor.capture());
    String actualPermissions = permissionCaptor.getValue();

    assertEquals(expectedPermissions, actualPermissions);
  }
}