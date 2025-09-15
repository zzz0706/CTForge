package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.JceAesCtrCryptoCodec;
import org.junit.Test;
import java.util.Arrays;
import static org.junit.Assert.assertNotEquals;

public class TestJceAesCtrCryptoCodec {

    // Prepare the input conditions for unit testing.
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
}