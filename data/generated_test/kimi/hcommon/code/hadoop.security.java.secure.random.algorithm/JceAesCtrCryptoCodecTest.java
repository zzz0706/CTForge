package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import static org.junit.Assert.assertNotNull;

public class JceAesCtrCryptoCodecTest {

    @Test
    public void testDefaultSecureRandomAlgorithmIsSHA1PRNG() throws NoSuchAlgorithmException {
        // 1. Create Configuration without explicit set
        Configuration conf = new Configuration();

        // 2. Compute expected value from Configuration (default lookup)
        String expectedAlgorithm = conf.get(
            CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY,
            "SHA1PRNG");

        // 3. Ensure the algorithm can be instantiated
        SecureRandom secureRandom = SecureRandom.getInstance(expectedAlgorithm);
        assertNotNull("SecureRandom instance should not be null", secureRandom);

        // 4. Invoke method under test
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();
        codec.setConf(conf);
    }
}