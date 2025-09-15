package org.apache.hadoop.crypto.key.kms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.crypto.key.kms.ValueQueue;
import org.apache.hadoop.crypto.key.kms.ValueQueue.QueueRefiller;
import org.junit.Test;

public class TestValueQueueInvalidConfigurationException {

    // Get configuration value using API
    private int getCacheSizeFromConfiguration() {
        Configuration conf = new Configuration();
        return conf.getInt(
            CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_SIZE,
            CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_SIZE_DEFAULT
        );
    }

    @Test
    public void testValueQueueInvalidConfigurationException() {
        // Prepare the input conditions for unit testing
        int cacheSize = getCacheSizeFromConfiguration();
        int invalidCacheSize = -1; // Set invalid size explicitly for exception test
        float invalidLowWatermark = 1.5f; // Low watermark > 1 (invalid)
        long invalidExpiry = 0; // Expiry <= 0 (invalid)
        int invalidThreads = -1; // Threads count <= 0 (invalid)

        try {
            // Attempt to construct ValueQueue with invalid configuration to trigger exception
            ValueQueue<Integer> queue = new ValueQueue<>(
                invalidCacheSize, invalidLowWatermark, invalidExpiry, invalidThreads,
                new QueueRefiller<Integer>() {
                    @Override
                    public void fillQueueForKey(String key, java.util.Queue<Integer> queue, int numToFill) {
                        // Mock refiller logic for the purpose of test
                        for (int i = 0; i < numToFill; i++) {
                            queue.add(1);
                        }
                    }
                }
            );
            // If no exception is thrown, the test fails
            assert false : "Expected IllegalArgumentException was not thrown.";
        } catch (IllegalArgumentException e) {
            // Check if the exception is correctly raised
            assert e.getMessage() != null : "Expected exception message is null.";
        }
    }
}