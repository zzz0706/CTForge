package org.apache.hadoop.crypto.random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import static org.junit.Assert.fail;

public class TestOsSecureRandom {
    
    @Test
    public void test_fillReservoir_invalidConfig() {
        // Obtain the configuration value using the API
        Configuration conf = new Configuration();
        String invalidDeviceFilePath = "/nonexistent/file/path"; // Simulate inaccessible file
        
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY, invalidDeviceFilePath);

        // Initialize instance of OsSecureRandom
        OsSecureRandom osSecureRandom = new OsSecureRandom();
        osSecureRandom.setConf(conf);

        try {
            // Invoke the fillReservoir method, which will eventually try to open the file
            // using the invalid path
            osSecureRandom.nextBytes(new byte[10]);
            fail("Expected an exception due to inaccessible secure random device file path.");
        } catch (RuntimeException e) {
            // Verify if the exception is due to inaccessible secure random device file
            if (!(e.getCause() instanceof IOException)) {
                fail("Expected IOException caused by inaccessible secure random device file path.");
            }
        }
    }
}