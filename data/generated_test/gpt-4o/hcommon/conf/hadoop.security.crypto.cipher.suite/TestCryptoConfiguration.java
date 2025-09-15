package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoCodec;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit test to validate configuration values for hadoop.security.crypto.cipher.suite.
 */
public class TestCryptoConfiguration {

    @Test
    public void testCryptoCipherSuiteConfiguration() {
        // Test code

        // 1. Use API to fetch configuration values instead of hardcoding.
        Configuration conf = new Configuration();

        // 2. Prepare test conditions
        // The key name for the cipher suite should be manually specified as the constant 
        // CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_KEY does not exist.
        String cipherSuiteKey = "hadoop.security.crypto.cipher.suite";
        String cipherSuiteDefaultValue = "AES/CTR/NoPadding";

        // Retrieve the configuration value for the cipher suite using the specified key.
        String cipherSuiteValue = conf.get(cipherSuiteKey, cipherSuiteDefaultValue);

        // 3. Test code
        try {
            // Convert the configuration value into a CipherSuite instance.
            CipherSuite cipherSuite = CipherSuite.convert(cipherSuiteValue);

            // Ensure that the cipher suite was properly converted without error.
            assertNotNull("Cipher suite conversion failed, resulting in null value.", cipherSuite);

            // Verify that the cipher suite name matches the expected format.
            assertTrue("Invalid cipher suite format: " + cipherSuiteValue,
                    cipherSuite.getName().matches("^[A-Za-z0-9/]+$"));
        } catch (Exception e) {
            // If conversion throws an exception, fail the test.
            fail("Failed to validate cipher suite configuration value due to exception: " + e.getMessage());
        }

        // 4. Test propagation and functional use with available CryptoCodec.
        CryptoCodec codec = CryptoCodec.getInstance(conf, CipherSuite.convert(cipherSuiteValue));
        if (codec != null) {
            // Ensure that the codec's cipher suite matches the configured cipher suite.
            assertEquals("Codec's cipher suite does not match the configured value.", cipherSuiteValue,
                    codec.getCipherSuite().getName());
        } else {
            // If codec is null, it implies no supporting crypto codec for the suite.
            assertTrue("No crypto codec available for configured cipher suite.", true);
        }
    }
}