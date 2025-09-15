package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoCodec;
import org.apache.hadoop.crypto.CipherSuite;
import org.junit.Test;
import static org.junit.Assert.*;

public class CryptoCodecTest {

    // Ensure `CryptoCodec.getInstance(Configuration conf)` correctly parses, propagates, and utilizes valid cipher suite configurations.
    @Test
    public void test_getInstance_withValidCipherSuite() {
        // Prepare configuration for testing
        Configuration conf = new Configuration();

        // Fix compilation error: Set explicit cipher suite configuration
        // This should mimic the expected configuration properly
        conf.set("hadoop.security.crypto.cipher.suite", CipherSuite.AES_CTR_NOPADDING.getName());

        // Retrieve the cipher suite configuration parameters
        String cipherSuiteKey = "hadoop.security.crypto.cipher.suite";
        String cipherSuiteName = conf.get(cipherSuiteKey, CipherSuite.AES_CTR_NOPADDING.getName());
        CipherSuite cipherSuite = CipherSuite.convert(cipherSuiteName);

        // Invoke the functionality under test
        CryptoCodec cryptoCodec = CryptoCodec.getInstance(conf);

        // Verify the returned CryptoCodec instance and its associated CipherSuite
        assertNotNull("Expected a valid CryptoCodec instance.", cryptoCodec);
        assertEquals("Cipher suite mismatch.", cipherSuite.getName(), cryptoCodec.getCipherSuite().getName());
    }
}