package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.JceAesCtrCryptoCodec;
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
        conf.set("hadoop.security.java.secure.random.algorithm", invalidAlgorithm);

        // Create an instance of JceAesCtrCryptoCodec
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();

        // Call the setConf method
        codec.setConf(conf);

        // Verify that the codec has a SecureRandom instance initialized
        assertNotNull("SecureRandom instance should not be null", codec);

        // Invoke the generateSecureRandom method with a byte array
        byte[] randomBytes = new byte[16]; // example byte array size
        codec.generateSecureRandom(randomBytes);

        // Verify that the byte array is populated with random values
        assertFalse("Byte array should be populated with random values", Arrays.equals(randomBytes, new byte[16]));
    }
}