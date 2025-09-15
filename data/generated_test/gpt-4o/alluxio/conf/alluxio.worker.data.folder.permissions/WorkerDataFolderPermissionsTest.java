package alluxio.conf;

import org.junit.Test;
import org.junit.Assert;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.AlluxioProperties;
import java.nio.file.attribute.PosixFilePermissions;

public class WorkerDataFolderPermissionsTest {

  @Test
  public void testWorkerDataFolderPermissionsConfiguration() {
    // Prepare the test configuration
    AlluxioProperties alluxioProperties = new AlluxioProperties();
    AlluxioConfiguration configuration = new InstancedConfiguration(alluxioProperties);
    alluxioProperties.set(PropertyKey.WORKER_DATA_FOLDER_PERMISSIONS, "rwxrwxrwx");

    /*
    * Step 1: Retrieve the value of the configuration `alluxio.worker.data.folder.permissions`
    * using the Alluxio 2.1.0 API correctly.
    */
    String permissions = configuration.get(PropertyKey.WORKER_DATA_FOLDER_PERMISSIONS);

    /*
    * Step 2: Verify whether the value of this configuration satisfies the constraints and dependencies:
    * - Check if the permission is a valid POSIX file permission string (e.g., "rwxrwxrwx").
    * - Use `PosixFilePermissions.fromString` to ensure proper format.
    * - Assert valid configuration.
    */
    boolean isValid = true;
    try {
      PosixFilePermissions.fromString(permissions); // This ensures the permissions string is valid.
    } catch (IllegalArgumentException e) {
      isValid = false;
    }

    // Assert the validity of the configuration.
    Assert.assertTrue("Invalid worker data folder permissions configuration: " + permissions, isValid);
  }
}