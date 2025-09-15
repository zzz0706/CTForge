package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoCodec;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;
import org.apache.hadoop.security.alias.JavaKeyStoreProvider;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

public class CryptoCodecTest {
    //test code

    @Test
    public void test_getInstance_withValidCipherSuiteOverloadedMethod() {
        // 1. Create a Configuration object
        Configuration conf = new Configuration();
        conf.set(JavaKeyStoreProvider.SCHEME_NAME + ".provider.path", "/tmp/test.jks");

        // 2. Set the default cipher suite key and value
        String defaultCipherSuiteKey = "hadoop.security.crypto.cipher.suite";
        String defaultCipherSuiteValue = "AES/CTR/NoPadding";
        conf.set(defaultCipherSuiteKey, defaultCipherSuiteValue);

        // 3. Get the default cipher suite value using API
        String defaultCipherSuiteName = conf.get(defaultCipherSuiteKey, defaultCipherSuiteValue);

        // 4. Convert the cipher suite name to CipherSuite
        CipherSuite defaultCipherSuite = CipherSuite.convert(defaultCipherSuiteName);

        // Ensure the cipher suite is valid and usable
        assertNotNull("CipherSuite should not be null", defaultCipherSuite);

        // 5. Invoke the getInstance(Configuration, CipherSuite) method
        CryptoCodec codec = CryptoCodec.getInstance(conf, defaultCipherSuite);

        // 6. Verify the returned codec is valid and matches the cipher suite
        assertNotNull("CryptoCodec instance should not be null", codec);
        assertEquals("CipherSuite names should match", defaultCipherSuite.getName(),
                codec.getCipherSuite().getName());
    }
}