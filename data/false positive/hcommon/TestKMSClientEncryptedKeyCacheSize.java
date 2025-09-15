package org.apache.hadoop.crypto.key.kms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestKMSClientEncryptedKeyCacheSize {

    @Test
    public void testKMSClientEncryptedKeyCacheSizeConfigurations() {
        Configuration conf = new Configuration();
        
        // Retrieve the configuration value
        int cacheSize = conf.getInt(
            CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_SIZE,
            500 // Default value
        );

        // Check constraints and dependencies
        // (based on source code: Preconditions.checkArgument(numValues >= 0,"\"numValues\" must be >= 0"))
        assertTrue(
            "Configuration 'hadoop.security.kms.client.encrypted.key.cache.size' must be greater than 0",
            cacheSize >= 0
        );
    }
}