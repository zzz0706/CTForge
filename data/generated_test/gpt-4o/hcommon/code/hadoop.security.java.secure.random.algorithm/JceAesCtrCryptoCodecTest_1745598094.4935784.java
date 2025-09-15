package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.JceAesCtrCryptoCodec;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;
import static org.junit.Assert.*;

public class JceAesCtrCryptoCodecTest {

    // Test method to verify that the setConf method correctly 
    // uses the specified algorithm and provider to initialize SecureRandom.
    @Test
    public void testSetConfWithProviderSpecified() {
        // Step 1: Create and configure the Configuration object.
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY, "SHA1PRNG");
        conf.set("hadoop.security.crypto.jce.provider", "SUN");

        // Step 2: Create an instance of JceAesCtrCryptoCodec and set the configuration.
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();
        codec.setConf(conf);

        // Step 3: Prepare a byte array and invoke the generateSecureRandom method.
        byte[] randomBytes = new byte[16];
        codec.generateSecureRandom(randomBytes);

        // Step 4: Check if the byte array is populated with random values.
        boolean isPopulated = false;
        for (byte b : randomBytes) {
            if (b != 0) {
                isPopulated = true;
                break;
            }
        }

        // Step 5: Assert the result matches expected behavior.
        assertTrue("The byte array was not populated with random values.", isPopulated);
    }
}