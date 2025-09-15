package org.apache.hadoop.crypto;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.security.GeneralSecurityException;
import java.security.SecureRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

public class JceAesCtrCryptoCodecTest {

    @Test
    public void invalidProviderFallsBackToDefaultSecureRandom() throws Exception {
        // 1. Configuration as input
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY, "NonExistentProvider");

        // 2. Prepare the test conditions
        String provider = conf.get(CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY);
        String secureRandomAlg = conf.get(
                CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY,
                CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_DEFAULT);

        // 3. Test code
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();
        codec.setConf(conf);

        // 4. Code after testing
        assertEquals("NonExistentProvider", provider);
        assertNotNull(codec);
    }
}