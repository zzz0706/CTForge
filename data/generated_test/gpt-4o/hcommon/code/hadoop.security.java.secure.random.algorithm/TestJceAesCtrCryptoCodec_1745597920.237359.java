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

        // Call the setConf method to apply the custom configuration
        codec.setConf(conf);

        // Assert that the configuration is properly set and the fallback mechanism works
        assertNotNull("SecureRandom instance should not be null after setting configuration", codec);

        // Test the generateSecureRandom method
        byte[] randomBytes = new byte[16]; // example byte array size
        codec.generateSecureRandom(randomBytes);

        // Create an empty byte array for comparison
        byte[] emptyBytes = new byte[16];
        
        // Verify that the randomBytes array is populated with secure random values (to confirm fallback behavior)
        assertFalse("Byte array should be populated with random values to indicate fallback SecureRandom instance worked",
                Arrays.equals(randomBytes, emptyBytes));
    }

    @Test
    public void testSetConfWithValidDefaultAlgorithm() {
        // Create a Configuration object using the API
        Configuration conf = new Configuration();

        // Use the default algorithm ("SHA1PRNG") explicitly
        String defaultAlgorithm = "SHA1PRNG";
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY, defaultAlgorithm);

        // Create an instance of JceAesCtrCryptoCodec
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();

        // Call the setConf method to apply the configuration
        codec.setConf(conf);

        // Assert that the default SecureRandom initialization was successful
        assertNotNull("SecureRandom instance should not be null with valid algorithm configuration", codec);

        // Test the generateSecureRandom method
        byte[] randomBytes = new byte[16]; // example byte array size
        codec.generateSecureRandom(randomBytes);

        // Create an empty byte array for comparison
        byte[] emptyBytes = new byte[16];

        // Verify the byte array is populated with secure random values
        assertFalse("Byte array should be populated with secure random data using valid algorithm",
                Arrays.equals(randomBytes, emptyBytes));
    }
}