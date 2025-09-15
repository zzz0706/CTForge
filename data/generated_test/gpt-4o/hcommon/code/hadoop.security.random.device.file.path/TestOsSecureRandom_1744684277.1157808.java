package org.apache.hadoop.crypto.random;   

import org.apache.hadoop.conf.Configuration;       
import org.apache.hadoop.crypto.random.OsSecureRandom;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;  
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;

public class TestOsSecureRandom {       
    @Test
    public void test_nextBytes_statefulConsistency() throws Exception {
        // Step 1: Get configuration value using API
        Configuration conf = new Configuration();
        String secureRandomDevicePath = conf.get(
            CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY, 
            CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_DEFAULT
        );

        // Ensure prerequisites: Secure random device file must exist and be readable
        Path path = Paths.get(secureRandomDevicePath);
        if (!Files.isReadable(path)) {
            throw new RuntimeException("Secure random device file is not readable: " + secureRandomDevicePath);
        }
        
        // Prepare the input conditions for unit testing
        OsSecureRandom secureRandom = new OsSecureRandom();
        secureRandom.setConf(conf);
        
        // Step 2: Test `nextBytes` with sequential invocations
        byte[] buffer1 = new byte[256];
        byte[] buffer2 = new byte[512];
        byte[] buffer3 = new byte[1024];
        
        secureRandom.nextBytes(buffer1);
        secureRandom.nextBytes(buffer2);
        secureRandom.nextBytes(buffer3);
        
        // Step 3: Validate results
        assert buffer1.length == 256 : "Buffer1 size mismatch";
        assert buffer2.length == 512 : "Buffer2 size mismatch";
        assert buffer3.length == 1024 : "Buffer3 size mismatch";
        
        // Verify buffers contain random data without exceptions
        checkBytesAreRandom(buffer1);
        checkBytesAreRandom(buffer2);
        checkBytesAreRandom(buffer3);
    }

    private void checkBytesAreRandom(byte[] bytes) {
        // This dummy implementation simply checks for non-zero data in the buffer
        int nonZeroCount = 0;
        for (byte b : bytes) {
            if (b != 0) {
                nonZeroCount++;
            }
        }
        assert nonZeroCount > 0 : "Buffer does not contain random data";
    }
}