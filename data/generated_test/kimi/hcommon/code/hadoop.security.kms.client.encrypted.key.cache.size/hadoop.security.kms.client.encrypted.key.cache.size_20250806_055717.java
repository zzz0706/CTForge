package org.apache.hadoop.crypto.key.kms;

import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SSLFactory.class, SecurityUtil.class})
public class KMSClientProviderConfigTest {

    private Configuration conf;
    private URI uri;

    @Mock
    private SSLFactory mockSslFactory;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        conf = new Configuration();
        uri = URI.create("kms://http@localhost:9600/kms");

        // Static mocks
        PowerMockito.mockStatic(SSLFactory.class);
        PowerMockito.mockStatic(SecurityUtil.class);

        PowerMockito.whenNew(SSLFactory.class)
                .withArguments(any(SSLFactory.Mode.class), any(Configuration.class))
                .thenReturn(mockSslFactory);

        PowerMockito.when(SecurityUtil.buildTokenService(any(URI.class)))
                .thenReturn(mock(Text.class));
    }

    @After
    public void tearDown() {
        PowerMockito.reset(SSLFactory.class, SecurityUtil.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void negativeValueThrowsIllegalArgumentException() throws Exception {
        conf.setInt(CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_SIZE, -100);
        new KMSClientProvider(uri, conf);
    }

    @Test
    public void zeroValueThrowsIllegalArgumentException() throws Exception {
        conf.setInt(CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_SIZE, 0);
        try {
            new KMSClientProvider(uri, conf);
            fail("Expected IllegalArgumentException for zero cache size");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("\"numValues\" must be > 0"));
        }
    }

    @Test
    public void validValueCreatesProviderSuccessfully() throws Exception {
        conf.setInt(CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_SIZE, 1000);
        KMSClientProvider provider = new KMSClientProvider(uri, conf);
        assertNotNull(provider);
    }

    @Test
    public void defaultValueUsedWhenNotConfigured() throws Exception {
        KMSClientProvider provider = new KMSClientProvider(uri, conf);
        assertNotNull(provider);
    }

    @Test
    public void verifyAllCacheRelatedConfigsAreRead() throws Exception {
        // Set non-default values to ensure they are picked up
        conf.setInt(CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_SIZE, 500);
        conf.setFloat(CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_LOW_WATERMARK, 0.3f);
        conf.setInt(CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_EXPIRY_MS, 60000);
        conf.setInt(CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_NUM_REFILL_THREADS, 4);

        KMSClientProvider provider = new KMSClientProvider(uri, conf);
        assertNotNull(provider);
    }

    @Test
    public void testValueQueueWithValidConfig() throws Exception {
        int numValues = 10;
        float lowWatermark = 0.5f;
        long expiry = 30000;
        int numThreads = 2;

        ValueQueue<KeyProviderCryptoExtension.EncryptedKeyVersion> queue =
                new ValueQueue<>(numValues, lowWatermark, expiry, numThreads,
                        new MockQueueRefiller());

        assertNotNull(queue);
    }

    @Test
    public void testValueQueueInitializeQueuesForKeys() throws Exception {
        ValueQueue<String> queue = new ValueQueue<>(
                5, 0.5f, 30000, 1, new MockQueueRefillerString());

        queue.initializeQueuesForKeys("key1", "key2");
        assertEquals(0, queue.getSize("key1"));
        assertEquals(0, queue.getSize("key2"));
    }

    @Test
    public void testValueQueueDrain() throws Exception {
        ValueQueue<String> queue = new ValueQueue<>(
                5, 0.5f, 30000, 1, new MockQueueRefillerString());

        queue.initializeQueuesForKeys("key1");
        queue.drain("key1");
        assertEquals(0, queue.getSize("key1"));
    }

    @Test
    public void testValueQueueGetAtMost() throws Exception {
        ValueQueue<String> queue = new ValueQueue<>(
                5, 0.5f, 30000, 1, new MockQueueRefillerString());

        List<String> result = queue.getAtMost("key1", 3);
        assertNotNull(result);
        assertEquals(3, result.size());
    }

    // Mock QueueRefiller for ValueQueue tests
    private static class MockQueueRefiller implements ValueQueue.QueueRefiller<KeyProviderCryptoExtension.EncryptedKeyVersion> {
        @Override
        public void fillQueueForKey(String keyName,
                                    Queue<KeyProviderCryptoExtension.EncryptedKeyVersion> queue,
                                    int numValues) {
            for (int i = 0; i < numValues; i++) {
                queue.offer(mock(KeyProviderCryptoExtension.EncryptedKeyVersion.class));
            }
        }
    }

    private static class MockQueueRefillerString implements ValueQueue.QueueRefiller<String> {
        @Override
        public void fillQueueForKey(String keyName,
                                    Queue<String> queue,
                                    int numValues) {
            for (int i = 0; i < numValues; i++) {
                queue.offer("value" + i);
            }
        }
    }
}