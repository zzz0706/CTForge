package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import java.security.GeneralSecurityException;
import java.security.SecureRandom;

import static org.junit.Assert.fail;

public class TestHadoopSecurityCryptoJceProvider {

    /**
     * Test the validity of the configuration value for 
     * "hadoop.security.crypto.jce.provider".
     */
    @Test
    public void testHadoopSecurityCryptoJceProviderConfig() {
        try {
            // Step 1: Load configuration and extract the value
            Configuration conf = new Configuration();
            String provider = conf.get(CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY);

            // Step 2: Verify constraints on "provider" value
            // The configuration value is optional. If it exists, it should be recognizable
            // as a Java JCE provider name. Otherwise, it can be null or empty.
            if (provider != null && !provider.isEmpty()) {
                // Step 3: Validate using SecureRandom to ensure the provider is valid
                try {
                    SecureRandom secureRandom = SecureRandom.getInstance("SHA1PRNG", provider);
                } catch (GeneralSecurityException e) {
                    // Fail the test if the provider is invalid
                    fail("Invalid JCE provider specified in configuration: " + provider);
                }
            }

            // If provider is null or empty, there is no error since it's an optional configuration.

        } catch (Exception e) {
            e.printStackTrace();
            fail("Unexpected error during configuration validation: " + e.getMessage());
        }
    }
}