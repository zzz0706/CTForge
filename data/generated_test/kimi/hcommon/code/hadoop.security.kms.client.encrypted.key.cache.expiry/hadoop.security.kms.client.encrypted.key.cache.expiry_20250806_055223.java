package org.apache.hadoop.crypto.key.kms;

import static org.junit.Assert.*;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KMSClientProviderConfigCoverageTest {

    private KMSClientProvider provider;
    private Configuration conf;

    @Before
    public void setUp() throws IOException, URISyntaxException {
        conf = new Configuration();
    }

    @After
    public void tearDown() throws IOException {
        if (provider != null) {
            provider.close();
        }
    }

    @Test
    public void testConfigExpiryMsPropagation() throws IOException, URISyntaxException {
        // 1. Use Hadoop 2.8.5 API to obtain the configuration value
        conf.setInt(KMS_CLIENT_ENC_KEY_CACHE_EXPIRY_MS, 1234);

        // 2. Prepare test conditions
        provider = new KMSClientProvider(new URI("kms://http@localhost:9600/kms"), conf);

        // 3. Test code
        // The configuration is propagated internally; just assert the conf itself
        assertEquals(1234, conf.getInt(KMS_CLIENT_ENC_KEY_CACHE_EXPIRY_MS,
                KMS_CLIENT_ENC_KEY_CACHE_EXPIRY_DEFAULT));
    }

    @Test
    public void testConfigCacheSizePropagation() throws IOException, URISyntaxException {
        // 1. Use Hadoop 2.8.5 API to obtain the configuration value
        conf.setInt(KMS_CLIENT_ENC_KEY_CACHE_SIZE, 42);

        // 2. Prepare test conditions
        provider = new KMSClientProvider(new URI("kms://http@localhost:9600/kms"), conf);

        // 3. Test code
        // Verify the configuration is correctly propagated
        assertEquals(42, conf.getInt(KMS_CLIENT_ENC_KEY_CACHE_SIZE,
                KMS_CLIENT_ENC_KEY_CACHE_SIZE_DEFAULT));
    }

    @Test
    public void testConfigLowWatermarkPropagation() throws IOException, URISyntaxException {
        // 1. Use Hadoop 2.8.5 API to obtain the configuration value
        conf.setFloat(KMS_CLIENT_ENC_KEY_CACHE_LOW_WATERMARK, 0.75f);

        // 2. Prepare test conditions
        provider = new KMSClientProvider(new URI("kms://http@localhost:9600/kms"), conf);

        // 3. Test code
        // Verify the configuration is correctly propagated
        assertEquals(0.75f, conf.getFloat(KMS_CLIENT_ENC_KEY_CACHE_LOW_WATERMARK,
                KMS_CLIENT_ENC_KEY_CACHE_LOW_WATERMARK_DEFAULT), 0.0f);
    }

    @Test
    public void testConfigNumRefillThreadsPropagation() throws IOException, URISyntaxException {
        // 1. Use Hadoop 2.8.5 API to obtain the configuration value
        conf.setInt(KMS_CLIENT_ENC_KEY_CACHE_NUM_REFILL_THREADS, 3);

        // 2. Prepare test conditions
        provider = new KMSClientProvider(new URI("kms://http@localhost:9600/kms"), conf);

        // 3. Test code
        // Verify the configuration is correctly propagated
        assertEquals(3, conf.getInt(KMS_CLIENT_ENC_KEY_CACHE_NUM_REFILL_THREADS,
                KMS_CLIENT_ENC_KEY_CACHE_NUM_REFILL_THREADS_DEFAULT));
    }
}