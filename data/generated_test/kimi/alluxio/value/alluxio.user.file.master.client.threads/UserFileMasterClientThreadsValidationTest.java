package alluxio.client.file;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioProperties;

import org.junit.Assert;
import org.junit.Test;

public class UserFileMasterClientThreadsValidationTest {

  @Test
  public void testUserFileMasterClientThreadsValid() {
    // 1. You need to use the alluxio2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    AlluxioConfiguration conf = new InstancedConfiguration(new AlluxioProperties());

    // 2. Prepare the test conditions.
    int threads = conf.getInt(alluxio.conf.PropertyKey.USER_FILE_MASTER_CLIENT_THREADS);

    // 3. Test code.
    Assert.assertTrue("alluxio.user.file.master.client.threads must be a positive integer",
        threads > 0);

    // 4. Code after testing: nothing to clean up
  }
}