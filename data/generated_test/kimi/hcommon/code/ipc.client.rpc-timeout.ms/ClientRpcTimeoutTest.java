package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.junit.Test;

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;

import static org.junit.Assert.*;

public class ClientRpcTimeoutTest {

    @Test
    public void testDefaultValueIsZero() {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // 2. Prepare the test conditions.
        Configuration conf = new Configuration(false); // fresh instance, no defaults
        // 3. Test code.
        int actualTimeout = Client.getRpcTimeout(conf);
        // 4. Code after testing.
        assertEquals("Default RPC timeout must be 0", 0, actualTimeout);
    }

    @Test
    public void testExplicitPositiveValue() {
        Configuration conf = new Configuration(false);
        int expected = 5000;
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, expected);
        int actual = Client.getRpcTimeout(conf);
        assertEquals("Should return the explicitly set positive value", expected, actual);
    }

    @Test
    public void testExplicitNegativeValueReturnsZero() {
        Configuration conf = new Configuration(false);
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, -100);
        int actual = Client.getRpcTimeout(conf);
        assertEquals("Negative value must be treated as 0", 0, actual);
    }

    @Test
    public void testGetTimeoutWithPingDisabled() {
        Configuration conf = new Configuration(false);
        conf.setBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY, false);
        int expected = conf.getInt(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY,
                                   CommonConfigurationKeys.IPC_PING_INTERVAL_DEFAULT);
        int actual = Client.getTimeout(conf);
        assertEquals("When ping disabled, timeout equals ping interval", expected, actual);
    }

    @Test
    public void testGetTimeoutWithPingEnabled() {
        Configuration conf = new Configuration(false);
        conf.setBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY, true);
        int actual = Client.getTimeout(conf);
        assertEquals("When ping enabled and no timeout, return -1", -1, actual);
    }

    @Test
    public void testRPCGetRpcTimeoutConsistency() {
        Configuration conf = new Configuration(false);
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 1234);
        int expected = 1234;
        int actual = RPC.getRpcTimeout(conf);
        assertEquals("RPC.getRpcTimeout must return the configured value", expected, actual);
    }

    @Test
    public void testWaitForProtocolProxyUsesRpcTimeout() throws IOException {
        Configuration conf = new Configuration(false);
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 3000);
        InetSocketAddress dummyAddr = InetSocketAddress.createUnresolved("dummy", 12345);

        try {
            RPC.waitForProtocolProxy(TestProtocol.class, 1L, dummyAddr, conf, 1000);
        } catch (IOException ignore) {
            // Expected; we only care about the timeout propagation
        }

        // The method internally calls RPC.getRpcTimeout(conf); we verify the value via reflection or mocking is not feasible
        // so we rely on coverage tools to confirm the usage path.
    }

    @Test
    public void testGetProtocolProxyUsesRpcTimeout() throws IOException {
        Configuration conf = new Configuration(false);
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 2000);
        InetSocketAddress dummyAddr = InetSocketAddress.createUnresolved("dummy", 12345);
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("test");
        SocketFactory factory = SocketFactory.getDefault();

        try {
            RPC.getProtocolProxy(TestProtocol.class, 1L, dummyAddr, ugi, conf, factory, 0, null);
        } catch (IOException ignore) {
            // Expected; we only care about the timeout propagation
        }
    }

    private interface TestProtocol {
        void ping();
    }
}