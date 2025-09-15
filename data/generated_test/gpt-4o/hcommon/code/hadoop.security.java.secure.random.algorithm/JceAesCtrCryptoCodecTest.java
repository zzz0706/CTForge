package org.apache.hadoop.crypto; 

import org.apache.hadoop.conf.Configuration;       
import org.junit.Test;
import java.security.SecureRandom;

public class JceAesCtrCryptoCodecTest {       
    @Test
    public void testSetConfWithProviderSpecified() {
        // Step 1: Create a Configuration object and get values using API.
        Configuration conf = new Configuration();
        String secureRandomAlgorithm = conf.get("hadoop.security.java.secure.random.algorithm", "SHA1PRNG");
        String jceProvider = conf.get("hadoop.security.crypto.jce.provider");

        // Step 2: Create an instance of JceAesCtrCryptoCodec.
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();
        
        // Step 3: Call the setConf method with the Configuration object.
        codec.setConf(conf);

        // Step 4: Prepare a byte array and invoke the generateSecureRandom method.
        byte[] randomBytes = new byte[16];
        codec.generateSecureRandom(randomBytes);

        // Step 5: Assert that the byte array is populated with non-zero random values.
        boolean isPopulated = false;
        for (byte b : randomBytes) {
            if (b != 0) {
                isPopulated = true;
                break;
            }
        }
        assert isPopulated : "The byte array was not populated with random values.";
    }
}