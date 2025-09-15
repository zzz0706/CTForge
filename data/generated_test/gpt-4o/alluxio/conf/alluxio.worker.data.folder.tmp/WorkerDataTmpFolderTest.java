package alluxio.worker;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.io.PathUtils;
import org.junit.Test;
import org.junit.Assert;
import java.io.File;

public class WorkerDataTmpFolderTest {

  @Test
  public void testWorkerDataTmpFolderConfig() {
    /*
     * Step 1: Based on the understood constraints and dependencies, determine whether the read 
     * configuration value satisfies the constraints and dependencies.
     * 
     * Step 2: Verify whether the value of this configuration item satisfies the constraints 
     * and dependencies.
     * - Check if the path is valid.
     * - Ensure the path is relative (as specified by the configuration description).
     * - Ensure compatibility with other configurations that depend on it.
     */

    // Read the configuration value using the Alluxio API
    String tmpFolder = ServerConfiguration.get(PropertyKey.WORKER_DATA_TMP_FOLDER);

    // Check if the configuration value is not null or empty
    Assert.assertNotNull("alluxio.worker.data.folder.tmp cannot be null", tmpFolder);
    Assert.assertFalse("alluxio.worker.data.folder.tmp cannot be empty", tmpFolder.isEmpty());

    // Validate that the path is relative
    File tmpFolderFile = new File(tmpFolder);
    Assert.assertTrue("alluxio.worker.data.folder.tmp must be a relative path",
        !tmpFolderFile.isAbsolute());

    // Validate path concatenation behavior
    String concatenatedPath = PathUtils.concatPath("/base_path", tmpFolder);
    Assert.assertTrue("Path concatenation failed or produced invalid output",
        concatenatedPath.startsWith("/base_path"));

    /*
     * Ensure compatibility with other configurations that depend on this property.
     * For example, check if PropertyKey.WORKER_DATA_TMP_SUBDIR_MAX is used together with
     * this configuration, as seen in tempPath method.
     */
    int subDirMax = ServerConfiguration.getInt(PropertyKey.WORKER_DATA_TMP_SUBDIR_MAX);
    Assert.assertTrue("alluxio.worker.data.tmp.subdir.max must be greater than 0", subDirMax > 0);
  }
}