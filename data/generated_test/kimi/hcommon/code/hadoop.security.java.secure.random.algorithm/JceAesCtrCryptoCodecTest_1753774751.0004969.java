package org.apache.hadoop.crypto;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.security.GeneralSecurityException;
import java.security.SecureRandom;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SecureRandom.class, JceAesCtrCryptoCodec.class})
public class JceAesCtrCryptoCodecTest {

    @Test
    public void testConfiguredAlgorithmWithProvider() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions.
        // Dynamically compute expected values from the configuration
        String expectedAlgorithm = conf.get(
                CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY,
                "SHA1PRNG");
        String expectedProvider = conf.get(
                CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY,
                "SUN");

        // 3. Test code.
        // Mock static SecureRandom.getInstance
        SecureRandom mockRandom = mock(SecureRandom.class);
        PowerMockito.mockStatic(SecureRandom.class);
        PowerMockito.when(SecureRandom.getInstance(expectedAlgorithm))
                .thenReturn(mockRandom);

        // Invoke setConf
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();
        codec.setConf(conf);

        // 4. Code after testing.
        // Verify static call parameters
        PowerMockito.verifyStatic(times(1));
        SecureRandom.getInstance(expectedAlgorithm);
    }
}