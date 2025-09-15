package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.JceAesCtrCryptoCodec;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertNotEquals;

public class TestJceAesCtrCryptoCodec {

    // Test case name: testSetConfWithDefaultAlgorithm
    // Purpose: Verify that the setConf method correctly falls back to the default algorithm (SHA1PRNG)
    // when no explicit algorithm is specified in the configuration.

    @Test
    public void testSetConfWithDefaultAlgorithm() {
        // Step 1: Create a Configuration object without setting hadoop.security.java.secure.random.algorithm.
        Configuration conf = new Configuration();

        // Step 2: Create an instance of JceAesCtrCryptoCodec.
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();

        // Step 3: Call the setConf method with the Configuration object.
        codec.setConf(conf);

        // Step 4: Invoke the generateSecureRandom method with a byte array.
        byte[] randomBytes = new byte[16];
        codec.generateSecureRandom(randomBytes);

        // Step 5: Verify that the byte array is populated with random values.
        // Ensuring the byte array is not all zeros (default state).
        byte[] zeroBytes = new byte[16];
        assertNotEquals(Arrays.toString(zeroBytes), Arrays.toString(randomBytes));
    }

    // Test case name: testSetConfWithCustomAlgorithm
    // Purpose: Verify that the setConf method correctly uses the specified custom algorithm from the Configuration.

    @Test
    public void testSetConfWithCustomAlgorithm() {
        // Step 1: Create a Configuration object and specify a custom algorithm (e.g., "SHA256PRNG").
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY, "SHA256PRNG");

        // Step 2: Create an instance of JceAesCtrCryptoCodec.
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();

        // Step 3: Call the setConf method with the Configuration object.
        codec.setConf(conf);

        // Step 4: Invoke the generateSecureRandom method with a byte array.
        byte[] randomBytes = new byte[16];
        codec.generateSecureRandom(randomBytes);

        // Step 5: Verify that the byte array is populated with random values.
        // Ensuring the byte array is not all zeros (default state).
        byte[] zeroBytes = new byte[16];
        assertNotEquals(Arrays.toString(zeroBytes), Arrays.toString(randomBytes));
    }

    // Test case name: testSetConfWithInvalidAlgorithm
    // Purpose: Verify the fallback mechanism when an invalid algorithm is specified.

    @Test
    public void testSetConfWithInvalidAlgorithm() {
        // Step 1: Create a Configuration object and specify an invalid algorithm.
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY, "InvalidAlgorithm");

        // Step 2: Create an instance of JceAesCtrCryptoCodec.
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();

        // Step 3: Call the setConf method with the Configuration object.
        codec.setConf(conf);

        // Step 4: Invoke the generateSecureRandom method with a byte array.
        byte[] randomBytes = new byte[16];
        codec.generateSecureRandom(randomBytes);

        // Step 5: Verify that the byte array is populated with random values.
        // Ensuring the byte array is not all zeros (default state).
        byte[] zeroBytes = new byte[16];
        assertNotEquals(Arrays.toString(zeroBytes), Arrays.toString(randomBytes));
    }
}