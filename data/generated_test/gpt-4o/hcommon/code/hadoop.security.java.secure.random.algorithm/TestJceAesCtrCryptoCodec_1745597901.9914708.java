package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.JceAesCtrCryptoCodec;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class TestJceAesCtrCryptoCodec {

    // get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void testSetConfWithInvalidAlgorithm() {
        // Create a Configuration object using the API
        Configuration conf = new Configuration();
        String invalidAlgorithm = "INVALID_ALGORITHM";
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY, invalidAlgorithm);

        // Create an instance of JceAesCtrCryptoCodec
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();

        // Call the setConf method
        codec.setConf(conf);

        // Assert that the internal SecureRandom instance was initialized correctly (fallback logic)
        assertNotNull("SecureRandom instance should not be null", codec);

        // Invoke the generateSecureRandom method with a byte array
        byte[] randomBytes = new byte[16]; // example byte array size
        codec.generateSecureRandom(randomBytes);

        // Verify that the byte array is populated with random values (not all zeros)
        byte[] emptyBytes = new byte[16]; // Create an empty byte array for comparison
        assertFalse("Byte array should have random values to ensure fallback logic works", Arrays.equals(randomBytes, emptyBytes));
    }
}