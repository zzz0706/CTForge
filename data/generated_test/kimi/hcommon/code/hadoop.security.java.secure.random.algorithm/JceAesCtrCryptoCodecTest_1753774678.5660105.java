package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertNotNull;

public class JceAesCtrCryptoCodecTest {

    @Test
    public void testInvalidAlgorithmFallsBackToDefaultSecureRandom() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY,
                 "InvalidAlgorithm");

        // 2. Prepare the test conditions.
        // JceAesCtrCryptoCodec will internally catch NoSuchAlgorithmException and fall back to new SecureRandom()

        // 3. Test code.
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();
        codec.setConf(conf);

        // 4. Code after testing.
        Field randomField = JceAesCtrCryptoCodec.class.getDeclaredField("random");
        randomField.setAccessible(true);
        assertNotNull(randomField.get(codec));
    }
}