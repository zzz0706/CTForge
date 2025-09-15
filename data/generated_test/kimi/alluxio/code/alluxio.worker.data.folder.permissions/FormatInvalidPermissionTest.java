package alluxio.cli;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServerConfiguration.class})
public class FormatInvalidPermissionTest {

  @Rule
  public TemporaryFolder mTempFolder = new TemporaryFolder();

  @Test(expected = RuntimeException.class)
  public void InvalidPermissionStringThrowsIllegalArgumentException() throws Exception {
    // 1. Use the alluxio2.1.0 API correctly to obtain configuration values
    //    – we will mock ServerConfiguration to return the desired values instead
    //    of creating a Configuration object directly (it does not exist in 2.1.0).

    // 2. Prepare the test conditions
    // Mock ServerConfiguration to return invalid permission string
    PowerMockito.mockStatic(ServerConfiguration.class);
    PowerMockito.when(ServerConfiguration.get(PropertyKey.WORKER_DATA_FOLDER_PERMISSIONS))
        .thenReturn("abc");

    // Create temporary worker data folder
    String workerDataFolder = mTempFolder.newFolder("workerData").getAbsolutePath();
    PowerMockito.when(ServerConfiguration.get(PropertyKey.WORKER_DATA_FOLDER))
        .thenReturn(workerDataFolder);

    // Mock tiered store levels
    PowerMockito.when(ServerConfiguration.getInt(PropertyKey.WORKER_TIERED_STORE_LEVELS))
        .thenReturn(1);
    PowerMockito.when(ServerConfiguration.get(
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(0)))
        .thenReturn(workerDataFolder);

    // 3. Test code – invoke Format.format(Mode.WORKER, null)
    //    Format.format expects ServerConfiguration to supply the values we mocked above.
    Format.format(Format.Mode.WORKER, null);

    // 4. Code after testing – nothing to do here
  }
}