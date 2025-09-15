package org.apache.hadoop.crypto.key.kms;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class KMSClientProviderTest {

    @Test
    public void verifyQueueExpiresAfterConfiguredExpiryTime() throws Exception {
        // 1. Configuration as input
        Configuration conf = new Configuration();

        // 2. Dynamic expected value calculation
        long expectedExpiry = conf.getLong(
                CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_EXPIRY_MS,
                CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_EXPIRY_DEFAULT);

        // 3. Mock/stub external dependencies
        ValueQueue.QueueRefiller<EncryptedKeyVersion> mockRefiller =
                mock(ValueQueue.QueueRefiller.class);

        // Create a CacheBuilder with the actual expiry time
        LoadingCache<String, LinkedBlockingQueue<EncryptedKeyVersion>> keyQueues =
                CacheBuilder.newBuilder()
                        .expireAfterAccess(expectedExpiry, TimeUnit.MILLISECONDS)
                        .build(new CacheLoader<String, LinkedBlockingQueue<EncryptedKeyVersion>>() {
                            @Override
                            public LinkedBlockingQueue<EncryptedKeyVersion> load(String keyName) {
                                return new LinkedBlockingQueue<>();
                            }
                        });

        // Create ValueQueue with the configuration-driven expiry
        ValueQueue<EncryptedKeyVersion> valueQueue =
                new ValueQueue<>(10, 0.5f, expectedExpiry, 1, mockRefiller);

        // Access the private keyQueues field via reflection to inject our test cache
        java.lang.reflect.Field keyQueuesField = ValueQueue.class.getDeclaredField("keyQueues");
        keyQueuesField.setAccessible(true);
        keyQueuesField.set(valueQueue, keyQueues);

        // 4. Invoke method under test
        String keyName = "k1";
        
        // Initialize queue for key 'k1'
        valueQueue.initializeQueuesForKeys(keyName);
        
        // Access immediately (should create queue)
        assertEquals(0, valueQueue.getSize(keyName)); // Queue exists but empty
        
        // 5. Advance time beyond expiry
        Thread.sleep(expectedExpiry + 1);
        
        // Attempt to access again - should trigger reload
        valueQueue.initializeQueuesForKeys(keyName);
        
        // Verify reload occurred (queue size resets to 0, proving eviction)
        assertEquals(0, valueQueue.getSize(keyName));
    }
}