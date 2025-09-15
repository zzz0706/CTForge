package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

public class CryptoCodecConfigTest {

    @Test
    public void testMultipleCodecClassesSelectsMatchingSuite() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions.
        String expectedSuite = conf.get(
                CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_KEY,
                CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_DEFAULT);

        // 3. Test code.
        CryptoCodec result = CryptoCodec.getInstance(conf, CipherSuite.convert(expectedSuite));

        // 4. Code after testing.
        assertNotNull(result);
        assertEquals(expectedSuite, result.getCipherSuite().getName());
    }
}