package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.random.OsSecureRandom;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class TestConfigurationValidation {

    /**
     * Test configuration validation for `hadoop.security.random.device.file.path`.
     */
    @Test
    public void testHadoopSecurityRandomDeviceFilePath() {
        // Step 1: Load the Hadoop configuration
        Configuration conf = new Configuration();

        // Step 2: Fetch the value of the configuration
        String randomDeviceFilePath = conf.get(
                CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY,
                CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_DEFAULT);

        // Step 3: Validate the configuration value against constraints
        // 1. Ensure the path is not null or empty
        Assert.assertNotNull("Configuration value for `hadoop.security.random.device.file.path` should not be null.", randomDeviceFilePath);
        Assert.assertFalse("Configuration value for `hadoop.security.random.device.file.path` should not be empty.", randomDeviceFilePath.isEmpty());

        // 2. Check if the path points to a valid file
        File randomDeviceFile = new File(randomDeviceFilePath);
        Assert.assertTrue("Configuration value for `hadoop.security.random.device.file.path` must point to an existing file.",
                randomDeviceFile.exists());
        
        // 3. Check if the file is readable (important for its usage in OsSecureRandom)
        Assert.assertTrue("Configuration value for `hadoop.security.random.device.file.path` must point to a readable file.",
                randomDeviceFile.canRead());

        // If all assertions pass, the configuration is correctly set
    }
}