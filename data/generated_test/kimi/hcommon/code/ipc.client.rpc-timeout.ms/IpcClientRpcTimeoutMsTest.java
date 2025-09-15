package org.apache.hadoop.ipc;
import static org.junit.Assert.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

public class IpcClientRpcTimeoutMsTest {

    @Test
    public void positiveValuePassedThrough() {
        // 1. Instantiate Configuration inside the test
        Configuration conf = new Configuration();

        // 2. Dynamic expected value – read the explicitly set value
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 1234);
        int expectedTimeout = conf.getInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY,
                                          CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_DEFAULT);

        // 3. No external dependencies to mock for this simple getter test

        // 4. Invoke the method under test
        int actualTimeout = Client.getRpcTimeout(conf);

        // 5. Assertion
        assertEquals("Positive value should be returned unchanged", expectedTimeout, actualTimeout);
    }
}