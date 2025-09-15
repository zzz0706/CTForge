package org.apache.hadoop.crypto;   

import org.apache.hadoop.conf.Configuration;       
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoCodec;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

public class TestCryptoCodec {

    /**
     * Test when an invalid CipherSuite is passed to the overloaded getInstance method.
     */
    @Test
    public void test_getInstance_withInvalidCipherSuiteOverloadedMethod() {
        // Prepare the test setup
        Configuration configuration = new Configuration(); // Create configuration object using API
        CipherSuite invalidCipherSuite = CipherSuite.UNKNOWN; // Use an invalid CipherSuite enumeration value

        // Invoke the method under test
        CryptoCodec result = CryptoCodec.getInstance(configuration, invalidCipherSuite);

        // Verify that the returned value is null, ensuring that no codec matches the invalid cipher suite
        assertNull("Expected null when no codec matches the invalid cipher suite.", result);
    }

    /**
     * Test when a valid cipher suite is passed via configuration key.
     */
    @Test
    public void test_getInstance_withValidCipherSuiteConfiguredValue() {
        // Prepare the test setup
        Configuration configuration = new Configuration();
        configuration.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_KEY,
                CipherSuite.AES_CTR_NOPADDING.getName()); // Use the API to get the proper cipher suite name

        // Invoke the method under test
        CryptoCodec result = CryptoCodec.getInstance(configuration);

        // Verify that the returned value is valid
        assertNotNull("Expected a valid CryptoCodec instance.", result);
        assertEquals("Expected the cipher suite to match the configured value.",
                CipherSuite.AES_CTR_NOPADDING, result.getCipherSuite());
    }

    /**
     * Test when an invalid cipher suite name is passed via configuration key.
     * Ensure IllegalArgumentException is thrown as per the API contract.
     */
    @Test(expected = IllegalArgumentException.class)
    public void test_getInstance_withInvalidConfigurationKey() {
        // Prepare the test setup
        Configuration configuration = new Configuration();
        configuration.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_KEY,
                "INVALID_CIPHER_SUITE"); // Pass an invalid cipher suite name

        // Invoke the method under test
        // Expect an IllegalArgumentException to be thrown, as the cipher suite name is invalid
        CryptoCodec.getInstance(configuration);
    }
}