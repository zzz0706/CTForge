package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import java.security.SecureRandom;
import static org.junit.Assert.assertTrue;

public class TestJceAesCtrCryptoCodec {
    @Test
    public void testSetConfWithNullProvider() {
        // Prepare the input conditions for unit testing.
        Configuration conf = new Configuration();
        String algorithm = conf.get("hadoop.security.java.secure.random.algorithm", "SHA1PRNG");

        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();

        // Call the setConf method with the prepared Configuration object.
        codec.setConf(conf);

        // Generate random bytes using generateSecureRandom method.
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