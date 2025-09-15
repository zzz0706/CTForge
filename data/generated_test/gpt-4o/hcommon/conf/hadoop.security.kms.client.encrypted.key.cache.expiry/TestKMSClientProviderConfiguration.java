package org.apache.hadoop.crypto.key.kms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestKMSClientProviderConfiguration {

    /**
     * Test to validate the configuration "hadoop.security.kms.client.encrypted.key.cache.expiry".
     * Verify the configured value satisfies the constraints as outlined in the source code.
     */
    @Test
    public void testEncryptedKeyCacheExpiryConfiguration() {
        Configuration conf = new Configuration();
        
        // Read the configuration value
        long cacheExpiry = conf.getLong(
            CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_EXPIRY_MS,
            CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_EXPIRY_DEFAULT
        );

        // Validate the value of cacheExpiry
        assertTrue("The expiration time of the cache must be > 0", cacheExpiry > 0);
    }
}