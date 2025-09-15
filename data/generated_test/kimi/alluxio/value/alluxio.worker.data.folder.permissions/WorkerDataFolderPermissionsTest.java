package alluxio.conf;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import org.junit.Test;

import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class WorkerDataFolderPermissionsTest {

  @Test
  public void testWorkerDataFolderPermissionsValid() {
    // 1. Use the Alluxio 2.1.0 API to obtain the configuration value
    AlluxioConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
    String permissions = conf.get(PropertyKey.WORKER_DATA_FOLDER_PERMISSIONS);

    // 2. Prepare the test conditions
    //    – no value is set in the test code; the test reads whatever is configured
    //    – we only verify the value satisfies documented constraints

    // 3. Test code
    try {
      Set<PosixFilePermission> perms = PosixFilePermissions.fromString(permissions);
      // The value must be a 9-character POSIX permission string
      assertTrue("Permission string must be exactly 9 characters",
          permissions.length() == 9);

      // Only characters r, w, x, - are allowed
      for (char c : permissions.toCharArray()) {
        assertTrue("Permission string contains invalid character: " + c,
            c == 'r' || c == 'w' || c == 'x' || c == '-');
      }

      // If short-circuit is used, 777 is required; otherwise any valid POSIX is allowed
      // We simply validate that the string can be parsed; actual enforcement is done at runtime
    } catch (IllegalArgumentException e) {
      fail("Invalid POSIX permission string: " + permissions);
    }

    // 4. Code after testing (nothing to clean up)
  }
}