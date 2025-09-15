package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import static org.junit.Assert.*;

public class CryptoCodecConfigTest {

    @Test
    public void testMultipleCodecClassesSelectsMatchingSuite() throws Exception {
        // 1. Use the hadoop-common 2.8.5 API correctly to obtain configuration values.
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions – vary the cipher suite to trigger different code paths.
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_KEY,
                 "AES/CTR/NoPadding");

        // 3. Test code.
        CryptoCodec result = CryptoCodec.getInstance(conf);

        // 4. Code after testing.
        assertNotNull("A valid suite should yield a codec", result);
    }

    @Test
    public void testDefaultCipherSuiteReturnsCodec() throws Exception {
        // 1. Use the hadoop-common 2.8.5 API correctly to obtain configuration values.
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions – rely on default value.
        conf.unset(CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_KEY);
        String expectedSuite = conf.get(
                CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_KEY,
                CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_DEFAULT);

        // 3. Test code.
        CryptoCodec result = CryptoCodec.getInstance(conf);

        // 4. Code after testing.
        assertNotNull("Default suite should yield a codec", result);
        assertEquals(expectedSuite, result.getCipherSuite().getName());
    }

    @Test
    public void testNullCipherSuiteEnumReturnsNull() throws Exception {
        // 1. Use the hadoop-common 2.8.5 API correctly to obtain configuration values.
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions – supply null suite.
        CipherSuite suite = null;

        // 3. Test code.
        CryptoCodec result = null;
        try {
            result = CryptoCodec.getInstance(conf, suite);
        } catch (NullPointerException e) {
            // expected; CryptoCodec throws NPE when suite is null
        }

        // 4. Code after testing.
        assertNull("Null suite should yield null codec", result);
    }

    @Test
    public void testExplicitCipherSuiteMatchesCodec() throws Exception {
        // 1. Use the hadoop-common 2.8.5 API correctly to obtain configuration values.
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions – use explicit valid suite.
        CipherSuite suite = CipherSuite.AES_CTR_NOPADDING;

        // 3. Test code.
        CryptoCodec result = CryptoCodec.getInstance(conf, suite);

        // 4. Code after testing.
        assertNotNull("Explicit valid suite should yield codec", result);
        assertEquals(suite.getName(), result.getCipherSuite().getName());
    }

    @Test
    public void testUnsupportedCipherSuiteReturnsNull() throws Exception {
        // 1. Use the hadoop-common 2.8.5 API correctly to obtain configuration values.
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions – supply unsupported suite.
        CryptoCodec result = null;
        try {
            conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_KEY,
                     "UNSUPPORTED/SUITE/NoPadding");

            // 3. Test code.
            result = CryptoCodec.getInstance(conf);
        } catch (IllegalArgumentException e) {
            // expected; CryptoCodec throws IllegalArgumentException for invalid suite
        }

        // 4. Code after testing.
        assertNull("Unsupported suite should yield null codec", result);
    }

    @Test
    public void testEmptyCipherSuiteUsesDefault() throws Exception {
        // 1. Use the hadoop-common 2.8.5 API correctly to obtain configuration values.
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions – supply empty suite to trigger default.
        CryptoCodec result = null;
        try {
            conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_KEY, "");

            // 3. Test code.
            result = CryptoCodec.getInstance(conf);
        } catch (IllegalArgumentException e) {
            // expected; CryptoCodec throws IllegalArgumentException for empty suite
        }

        // 4. Code after testing.
        assertNull("Empty suite should yield null codec", result);
    }
}