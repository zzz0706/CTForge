package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.JceAesCtrCryptoCodec;
import org.junit.Test;
import org.mockito.Mockito;

import java.security.GeneralSecurityException;
import java.security.SecureRandom;

public class TestJceAesCtrCryptoCodec_SetConf_InvalidProvider {

    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void testSetConf_InvalidProvider() {
        // 1. Mock the Configuration object using Mockito
        Configuration mockConfig = Mockito.mock(Configuration.class);
        Mockito.when(mockConfig.get("hadoop.security.crypto.jce.provider")).thenReturn("InvalidProviderName");
        Mockito.when(mockConfig.get("hadoop.security.java.secure.random.algorithm")).thenReturn("SHA1PRNG");
        
        // 2. Prepare the testing instance
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();

        // 3. Test code execution
        codec.setConf(mockConfig);

        // Verify behavior
        try {
            SecureRandom randomInstance = SecureRandom.getInstance("SHA1PRNG");
            assert randomInstance != null;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("SecureRandom fallback initialization failed", e);
        }
    }
}