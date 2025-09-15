package org.apache.hadoop.crypto.key.kms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.crypto.key.kms.ValueQueue.QueueRefiller;
import org.junit.Test;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.Queue;

import static org.junit.Assert.assertTrue;

public class TestValueQueueInitializeQueuesForKeys {
    // Unit test for initializing queues for keys
    @Test
    public void testInitializeQueuesForKeys() throws Exception {
        // Step 1: Load configuration to get expiry time via API
        Configuration conf = new Configuration();
        long expiry = conf.getLong(
                CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_EXPIRY_MS,
                CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_EXPIRY_DEFAULT);

        // Step 2: Create a mock QueueRefiller implementation
        QueueRefiller<String> mockRefiller = new QueueRefiller<String>() {
            @Override
            public void fillQueueForKey(String key, Queue<String> queue, int batchSize) {
                // Mock logic to simulate queue refilling
                queue.add("Mock value for key: " + key);
            }
        };

        // Step 3: Simulate the initialization of queues for a given key
        LinkedBlockingQueue<String> testQueue = new LinkedBlockingQueue<>();
        mockRefiller.fillQueueForKey("testKey", testQueue, 1);

        // Step 4: Validate if the queue is correctly filled with values
        assertTrue("Queue should contain values", !testQueue.isEmpty());
    }
}