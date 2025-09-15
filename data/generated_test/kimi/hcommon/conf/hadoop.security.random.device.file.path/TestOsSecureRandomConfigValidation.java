package org.apache.hadoop.crypto.random;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

public class TestOsSecureRandomConfigValidation {

  @Test
  public void testHadoopSecurityRandomDeviceFilePathValid() {
    Configuration conf = new Configuration(false);
    // 1. Obtain configuration value via API
    String randomDevPath = conf.get(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY,
        CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_DEFAULT);

    // 2. Prepare test conditions
    File deviceFile = new File(randomDevPath);

    // 3. Test code
    boolean isValid = deviceFile.exists() && deviceFile.canRead();

    // 4. Code after testing
    assertTrue("Configured OS security random device file path must exist and be readable: " + randomDevPath,
        isValid);
  }

  @Test
  public void testHadoopSecurityRandomDeviceFilePathNotEmpty() {
    Configuration conf = new Configuration(false);
    // 1. Obtain configuration value via API
    String randomDevPath = conf.get(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY,
        CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_DEFAULT);

    // 2. Prepare test conditions (no additional setup needed)

    // 3. Test code
    boolean isNotEmpty = randomDevPath != null && !randomDevPath.trim().isEmpty();

    // 4. Code after testing
    assertTrue("Configured OS security random device file path must not be null or empty", isNotEmpty);
  }
}