package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class IpcClientRpcTimeoutTest {

    @Test
    public void negativeValueClampedToZero() {
        // 1. Instantiate configuration
        Configuration conf = new Configuration();
        
        // 2. Set negative value to test clamping
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, -500);
        
        // 3. Calculate expected value (should be 0 for negative input)
        long expectedTimeout = 0;
        
        // 4. Invoke method under test
        int actualTimeout = Client.getRpcTimeout(conf);
        
        // 5. Assert the result
        assertEquals(expectedTimeout, actualTimeout);
    }
}