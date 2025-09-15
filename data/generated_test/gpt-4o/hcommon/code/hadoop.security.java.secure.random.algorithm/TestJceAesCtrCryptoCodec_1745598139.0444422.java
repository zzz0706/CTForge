package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.JceAesCtrCryptoCodec;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestJceAesCtrCryptoCodec {
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

    @Test
    public void testSetConfWithValidProvider() {
        // Prepare the input conditions with a valid provider.
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
}