package org.apache.hadoop.crypto;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.security.GeneralSecurityException;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.Security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

public class JceAesCtrCryptoCodecConfigTest {

    @Test
    public void testSetConfInvalidProvider() throws Exception {
        // 1. You need to use the hadoop-common 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY, "NonExistentProvider");

        // 2. Prepare the test conditions.
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();

        // 3. Test code.
        codec.setConf(conf);

        // 4. Code after testing.
        SecureRandom random = getSecureRandom(codec);
        assertNotNull(random);
        assertEquals("SHA1PRNG", random.getAlgorithm());
    }

    @Test
    public void testSetConfValidProvider() throws Exception {
        // 1. You need to use the hadoop-common 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY, "SUN");

        // 2. Prepare the test conditions.
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();

        // 3. Test code.
        codec.setConf(conf);

        // 4. Code after testing.
        SecureRandom random = getSecureRandom(codec);
        assertNotNull(random);
        Provider actualProvider = random.getProvider();
        assertEquals("SUN", actualProvider.getName());
    }

    @Test
    public void testSetConfNullProvider() throws Exception {
        // 1. You need to use the hadoop-common 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        conf.unset(CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY);

        // 2. Prepare the test conditions.
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();

        // 3. Test code.
        codec.setConf(conf);

        // 4. Code after testing.
        SecureRandom random = getSecureRandom(codec);
        assertNotNull(random);
    }

    @Test
    public void testSetConfCustomSecureRandomAlgorithm() throws Exception {
        // 1. You need to use the hadoop-common 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY, "NativePRNG");

        // 2. Prepare the test conditions.
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();

        // 3. Test code.
        codec.setConf(conf);

        // 4. Code after testing.
        SecureRandom random = getSecureRandom(codec);
        assertNotNull(random);
        assertEquals("NativePRNG", random.getAlgorithm());
    }

    @Test
    public void testSetConfInvalidAlgorithmFallback() throws Exception {
        // 1. You need to use the hadoop-common 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY, "InvalidAlgorithm");

        // 2. Prepare the test conditions.
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();

        // 3. Test code.
        codec.setConf(conf);

        // 4. Code after testing.
        SecureRandom random = getSecureRandom(codec);
        assertNotNull(random);
        assertEquals("SHA1PRNG", random.getAlgorithm());
    }

    // Helper method to access the private field via reflection
    private SecureRandom getSecureRandom(JceAesCtrCryptoCodec codec) throws Exception {
        java.lang.reflect.Field field = JceAesCtrCryptoCodec.class.getDeclaredField("random");
        field.setAccessible(true);
        return (SecureRandom) field.get(codec);
    }
}