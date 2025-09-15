package org.apache.hadoop.crypto.key.kms;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;
import org.junit.Test;

public class KMSClientProviderTest {

    @Test(expected = IllegalArgumentException.class)
    public void verifyNegativeExpiryRejection() throws IOException, URISyntaxException {
        // 1. Prepare configuration with negative expiry
        Configuration conf = new Configuration();
        conf.setInt(CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_EXPIRY_MS, -1);

        // 2. Attempt to construct KMSClientProvider
        new KMSClientProvider(new URI("kms://http@localhost:9600/kms"), conf);
    }
}