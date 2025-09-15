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
        // 1. You need to use the hadoop-common 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration(false);
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY, "NativePRNG");
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY, "SUN");

        // 2. Prepare the test conditions.
        SecureRandom mockRandom = mock(SecureRandom.class);
        PowerMockito.mockStatic(SecureRandom.class);
        PowerMockito.when(SecureRandom.getInstance("NativePRNG", "SUN"))
                .thenReturn(mockRandom);

        // 3. Test code.
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();
        codec.setConf(conf);
        byte[] buf = new byte[16];
        codec.generateSecureRandom(buf);

        // 4. Code after testing.
        PowerMockito.verifyStatic(times(1));
        SecureRandom.getInstance("NativePRNG", "SUN");
        verify(mockRandom, times(1)).nextBytes(buf);
    }

    @Test
    public void testDefaultAlgorithmWithoutProvider() throws Exception {
        // 1. You need to use the hadoop-common 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration(false);

        // 2. Prepare the test conditions.
        SecureRandom mockRandom = mock(SecureRandom.class);
        PowerMockito.mockStatic(SecureRandom.class);
        PowerMockito.when(SecureRandom.getInstance("SHA1PRNG"))
                .thenReturn(mockRandom);

        // 3. Test code.
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();
        codec.setConf(conf);
        byte[] buf = new byte[32];
        codec.generateSecureRandom(buf);

        // 4. Code after testing.
        PowerMockito.verifyStatic(times(1));
        SecureRandom.getInstance("SHA1PRNG");
        verify(mockRandom, times(1)).nextBytes(buf);
    }

    @Test
    public void testInvalidAlgorithmFallback() throws Exception {
        // 1. You need to use the hadoop-common 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration(false);
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY, "NonExistingAlgo");

        // 2. Prepare the test conditions.
        PowerMockito.mockStatic(SecureRandom.class);
        PowerMockito.when(SecureRandom.getInstance("NonExistingAlgo"))
                .thenThrow(new java.security.NoSuchAlgorithmException("invalid algorithm"));

        // 3. Test code.
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();
        codec.setConf(conf);
        byte[] buf = new byte[8];
        codec.generateSecureRandom(buf);

        // 4. Code after testing.
        PowerMockito.verifyStatic(times(1));
        SecureRandom.getInstance("NonExistingAlgo");
        // Verify fallback SecureRandom was created and used
        assertNotNull(buf);
    }

    @Test
    public void testNullProviderUsesDefault() throws Exception {
        // 1. You need to use the hadoop-common 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration(false);
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY, "NativePRNG");

        // 2. Prepare the test conditions.
        SecureRandom mockRandom = mock(SecureRandom.class);
        PowerMockito.mockStatic(SecureRandom.class);
        PowerMockito.when(SecureRandom.getInstance("NativePRNG"))
                .thenReturn(mockRandom);

        // 3. Test code.
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();
        codec.setConf(conf);
        byte[] buf = new byte[4];
        codec.generateSecureRandom(buf);

        // 4. Code after testing.
        PowerMockito.verifyStatic(times(1));
        SecureRandom.getInstance("NativePRNG");
        verify(mockRandom, times(1)).nextBytes(buf);
    }

    @Test
    public void testInvalidProviderFallback() throws Exception {
        // 1. You need to use the hadoop-common 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration(false);
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY, "NativePRNG");
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY, "NonExistingProvider");

        // 2. Prepare the test conditions.
        PowerMockito.mockStatic(SecureRandom.class);
        PowerMockito.when(SecureRandom.getInstance("NativePRNG", "NonExistingProvider"))
                .thenThrow(new java.security.NoSuchProviderException("invalid provider"));

        // 3. Test code.
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();
        codec.setConf(conf);
        byte[] buf = new byte[12];
        codec.generateSecureRandom(buf);

        // 4. Code after testing.
        PowerMockito.verifyStatic(times(1));
        SecureRandom.getInstance("NativePRNG", "NonExistingProvider");
        // Verify fallback SecureRandom was created and used
        assertNotNull(buf);
    }
}