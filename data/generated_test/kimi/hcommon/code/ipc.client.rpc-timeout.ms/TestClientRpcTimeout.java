package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestClientRpcTimeout {

    @Test
    public void legacyGetTimeoutDelegatesToGetRpcTimeout() {
        // 1. Create Configuration instance
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions
        conf.setBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY, true);
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 2000);

        // 3. Invoke the method under test
        int timeout = Client.getTimeout(conf);

        // 4. Compute expected value dynamically
        long expectedTimeout = conf.getInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY,
                                           CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_DEFAULT);

        // 5. Assert the result
        assertEquals(expectedTimeout, timeout);
    }
}