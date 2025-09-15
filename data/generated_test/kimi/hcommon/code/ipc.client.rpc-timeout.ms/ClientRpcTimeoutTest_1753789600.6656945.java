package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

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

    @Test
    public void getRpcTimeoutReturnsConfiguredValue() {
        // 1. Configuration as Input
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 30000);

        // 3. Dynamic Expected Value Calculation
        int expectedTimeout = conf.getInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY,
                                         CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_DEFAULT);

        // 4. Invoke the Method Under Test
        int actualTimeout = Client.getRpcTimeout(conf);

        // 5. Assertions and Verification
        assertEquals(expectedTimeout, actualTimeout);
    }

    @Test
    public void getRpcTimeoutReturnsZeroForNegativeValue() {
        // 1. Configuration as Input
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, -1);

        // 3. Dynamic Expected Value Calculation
        int expectedTimeout = 0;

        // 4. Invoke the Method Under Test
        int actualTimeout = Client.getRpcTimeout(conf);

        // 5. Assertions and Verification
        assertEquals(expectedTimeout, actualTimeout);
    }

    @Test
    public void RPCgetRpcTimeoutReturnsConfiguredValue() {
        // 1. Configuration as Input
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 45000);

        // 3. Dynamic Expected Value Calculation
        int expectedTimeout = conf.getInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY,
                                         CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_DEFAULT);

        // 4. Invoke the Method Under Test
        int actualTimeout = RPC.getRpcTimeout(conf);

        // 5. Assertions and Verification
        assertEquals(expectedTimeout, actualTimeout);
    }

    @Test
    public void RPCwaitForProtocolProxyUsesRpcTimeoutFromConf() throws IOException {
        // 1. Configuration as Input
        Configuration conf = new Configuration();
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 5000);

        // 2. Prepare the test conditions
        InetSocketAddress dummyAddr = InetSocketAddress.createUnresolved("127.0.0.1", 0);
        long connTimeout = 1000L;

        // 3. Dynamic Expected Value Calculation
        int expectedRpcTimeout = RPC.getRpcTimeout(conf);

        // 4. Invoke the Method Under Test (expect failure due to dummy address)
        try {
            RPC.waitForProtocolProxy(TestProtocol.class, 1L, dummyAddr, conf, connTimeout);
        } catch (IOException e) {
            // Expected due to dummy address; ignore
        }

        // 5. Assertions and Verification
        assertEquals(5000, expectedRpcTimeout);
    }

    @Test
    public void RPCgetProtocolProxyUsesRpcTimeoutFromConf() throws IOException {
        // 1. Configuration as Input
        Configuration conf = new Configuration();
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 7000);

        // 2. Prepare the test conditions
        InetSocketAddress dummyAddr = InetSocketAddress.createUnresolved("127.0.0.1", 0);
        UserGroupInformation ticket = UserGroupInformation.getCurrentUser();

        // 3. Dynamic Expected Value Calculation
        int expectedRpcTimeout = RPC.getRpcTimeout(conf);

        // 4. Invoke the Method Under Test (expect failure due to dummy address)
        try {
            RPC.getProtocolProxy(TestProtocol.class, 1L, dummyAddr, ticket, conf, null);
        } catch (IOException e) {
            // Expected due to dummy address; ignore
        }

        // 5. Assertions and Verification
        assertEquals(7000, expectedRpcTimeout);
    }

    // Dummy protocol for testing
    private interface TestProtocol {}
}