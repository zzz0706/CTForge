package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.security.GeneralSecurityException;
import java.security.SecureRandom;

import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SecureRandom.class, JceAesCtrCryptoCodec.class})
public class JceAesCtrCryptoCodecTest {

    @Test
    public void testGenerateSecureRandomUsesConfiguredAlgorithm() throws Exception {
        // 1. Create Configuration and set the required algorithm using the public key
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY, "SHA1PRNG");

        // 2. Prepare test conditions: mock SecureRandom and static factory
        SecureRandom mockSecureRandom = mock(SecureRandom.class);
        PowerMockito.mockStatic(SecureRandom.class);
        PowerMockito.when(SecureRandom.getInstance("SHA1PRNG")).thenReturn(mockSecureRandom);

        // 3. Test code: instantiate codec, inject configuration, and exercise generateSecureRandom
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();
        codec.setConf(conf);

        byte[] buffer = new byte[16];
        codec.generateSecureRandom(buffer);

        // 4. Verify interaction: ensure the mock SecureRandom.nextBytes was invoked exactly once with the buffer
        verify(mockSecureRandom, times(1)).nextBytes(buffer);
    }
}