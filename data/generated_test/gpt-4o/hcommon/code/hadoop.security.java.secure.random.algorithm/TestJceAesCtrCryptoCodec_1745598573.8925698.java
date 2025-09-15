package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.JceAesCtrCryptoCodec;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestJceAesCtrCryptoCodec {

    /**
     * Test setConf method with only the secure random algorithm set and no provider.
     */
    @Test
    public void testSetConfWithNullProvider() {
        // Prepare the input conditions for unit testing.
        Configuration conf = new Configuration();
        conf.set("hadoop.security.java.secure.random.algorithm", "SHA1PRNG");

        // Create an instance of JceAesCtrCryptoCodec and set the configuration.
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();
        codec.setConf(conf);

        // Generate random bytes using the generateSecureRandom method.
        byte[] randomBytes = new byte[16];
        codec.generateSecureRandom(randomBytes);

        // Verify that the byte array is non-zero populated.
        boolean isPopulated = false;
        for (byte b : randomBytes) {
            if (b != 0) {
                isPopulated = true;
                break;
            }
        }
        assertTrue("SecureRandom bytes should be non-zero populated", isPopulated);
    }

    /**
     * Test setConf method with a valid provider explicitly set.
     */
    @Test
    public void testSetConfWithValidProvider() {
        // Prepare the input conditions by setting both the secure random algorithm and valid provider.
        Configuration conf = new Configuration();
        conf.set("hadoop.security.java.secure.random.algorithm", "SHA1PRNG");
        conf.set("hadoop.security.crypto.jce.provider", "SUN");

        // Create an instance of JceAesCtrCryptoCodec and set the configuration.
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();
        codec.setConf(conf);

        // Generate random bytes using the generateSecureRandom method.
        byte[] randomBytes = new byte[16];
        codec.generateSecureRandom(randomBytes);

        // Verify that the byte array is non-zero populated.
        boolean isPopulated = false;
        for (byte b : randomBytes) {
            if (b != 0) {
                isPopulated = true;
                break;
            }
        }
        assertTrue("SecureRandom bytes should be non-zero populated", isPopulated);
    }

    /**
     * Test setConf method with an invalid algorithm to verify fallback behavior.
     */
    @Test
    public void testSetConfWithInvalidAlgorithm() {
        // Prepare the input conditions with an invalid secure random algorithm value.
        Configuration conf = new Configuration();
        conf.set("hadoop.security.java.secure.random.algorithm", "INVALID_ALGO");

        // Create an instance of JceAesCtrCryptoCodec and set the configuration.
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();
        codec.setConf(conf);

        // Generate random bytes using the fallback random instance.
        byte[] randomBytes = new byte[16];
        codec.generateSecureRandom(randomBytes);

        // Verify that the byte array is non-zero populated.
        boolean isPopulated = false;
        for (byte b : randomBytes) {
            if (b != 0) {
                isPopulated = true;
                break;
            }
        }
        assertTrue("SecureRandom bytes should be non-zero populated even when using fallback", isPopulated);
    }
}