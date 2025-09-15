package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.security.SecureRandom;

import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SecureRandom.class, JceAesCtrCryptoCodec.class})
public class JceAesCtrCryptoCodecTest {

    @Test
    public void testGenerateSecureRandomUsesConfiguredAlgorithm() throws Exception {
        // 1. Instantiate Configuration to read the default
        Configuration conf = new Configuration();

        // 2. Compute expected algorithm from configuration
        String expectedAlgorithm = conf.get(
                CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY,
                "SHA1PRNG");

        // 3. Prepare test conditions: mock SecureRandom
        SecureRandom mockSecureRandom = mock(SecureRandom.class);
        PowerMockito.mockStatic(SecureRandom.class);
        PowerMockito.when(SecureRandom.getInstance(expectedAlgorithm)).thenReturn(mockSecureRandom);

        // 4. Test code
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();
        codec.setConf(conf);

        byte[] buffer = new byte[16];
        codec.generateSecureRandom(buffer);

        // 5. Verify interaction
        verify(mockSecureRandom, times(1)).nextBytes(buffer);
    }
}