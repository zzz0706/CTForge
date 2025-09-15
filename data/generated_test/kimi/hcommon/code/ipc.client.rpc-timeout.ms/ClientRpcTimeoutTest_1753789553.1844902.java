package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ClientRpcTimeoutTest {

    @Test
    public void legacyGetTimeoutReturnsPingIntervalWhenPingDisabled() {
        // 1. Configuration as Input
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions
        conf.setBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY, false);
        conf.setInt(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY, 60000);

        // 3. Dynamic Expected Value Calculation
        long expectedTimeout = conf.getInt(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY,
                                         CommonConfigurationKeys.IPC_PING_INTERVAL_DEFAULT);

        // 4. Invoke the Method Under Test
        long actualTimeout = Client.getTimeout(conf);

        // 5. Assertions and Verification
        assertEquals(expectedTimeout, actualTimeout);
    }
}