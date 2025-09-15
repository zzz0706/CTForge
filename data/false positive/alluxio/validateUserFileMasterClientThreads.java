package alluxio.conf;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.PropertyKey;
import org.junit.Assert;
import org.junit.Test;

public class validateUserFileMasterClientThreads {

  @Test
  public void validateUserFileMasterClientThreadsConfiguration() {
    /*
     * Test steps:
     * 1. Prepare the test environment: Initialize `AlluxioProperties` and `InstancedConfiguration`.
     * 2. Use the correct API to fetch the configuration value.
     * 3. Validate the fetched configuration value.
     */

    // Step 1: Prepare the test environment
    // Create AlluxioProperties and populate it with test configuration data
    AlluxioProperties properties = new AlluxioProperties();
    properties.set(PropertyKey.USER_FILE_MASTER_CLIENT_THREADS, "10");

    // Pass the properties to InstancedConfiguration
    AlluxioConfiguration configuration = new InstancedConfiguration(properties);

    // Step 2: Use the configuration API to fetch the value
    int fileMasterClientThreads = configuration.getInt(PropertyKey.USER_FILE_MASTER_CLIENT_THREADS);

    // Step 3: Validate the fetched value
    // Ensure the value is a non-negative integer
    Assert.assertTrue("Configuration 'alluxio.user.file.master.client.threads' must be non-negative.",
        fileMasterClientThreads >= 0);

    Assert.assertEquals(10, fileMasterClientThreads);
  }
}