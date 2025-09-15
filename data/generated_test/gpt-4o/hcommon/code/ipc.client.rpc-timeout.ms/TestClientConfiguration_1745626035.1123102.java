package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.ProtocolProxy;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import javax.net.SocketFactory;

import static org.junit.Assert.assertEquals;

public class TestClientConfiguration {

    // Test code covering 'Client.getRpcTimeout(Configuration conf)'
    @Test
    public void testClient_getRpcTimeout() {
        // 1. Create a Configuration object
        Configuration conf = new Configuration();

        // 2. Set a custom RPC timeout
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 5000);

        // 3. Retrieve the timeout value using the API
        int rpcTimeout = Client.getRpcTimeout(conf);

        // 4. Validate the value retrieved
        assertEquals("RPC timeout does not match the configuration value!", 5000, rpcTimeout);
    }

    // Test code covering 'Client.getTimeout(Configuration conf)'
    @Test
    public void testClient_getTimeout_withPingDisabled() {
        // 1. Create a Configuration object
        Configuration conf = new Configuration();

        // 2. Disable ping
        conf.setBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY, false);

        // 3. Retrieve the timeout value using the API
        int actualTimeout = Client.getTimeout(conf);

        // 4. Ensure the timeout matches ping interval when 'ipc.client.ping' is disabled
        int expectedTimeout = Client.getPingInterval(conf);
        assertEquals("Timeout does not match the expected ping interval!", expectedTimeout, actualTimeout);
    }

    // Test code covering 'RPC.getRpcTimeout(Configuration conf)'
    @Test
    public void testRPC_getRpcTimeout() {
        // 1. Create a Configuration object
        Configuration conf = new Configuration();

        // 2. Set a RPC timeout value
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 7000);

        // 3. Retrieve the value using RPC API
        int rpcTimeout = RPC.getRpcTimeout(conf);

        // 4. Validate the value fetched
        assertEquals("RPC timeout does not match the configuration value!", 7000, rpcTimeout);
    }

    // Test code covering 'RPC.waitForProtocolProxy'
    @Test
    public void testRPC_waitForProtocolProxy() throws IOException {
        // 1. Create a Configuration object
        Configuration conf = new Configuration();

        // 2. Mock required input data
        InetSocketAddress addr = new InetSocketAddress("localhost", 8080);
        Class<TestProtocolImpl> protocol = TestProtocolImpl.class; // Use an actual implementation class for testing
        long clientVersion = 1L;
        long connTimeout = 2000L;

        // 3. Call the method to ensure it uses the configuration timeout
        ProtocolProxy<TestProtocolImpl> proxy = RPC.waitForProtocolProxy(protocol, clientVersion, addr, conf, connTimeout);

        // 4. Validate the output is not null (proxies cannot be fully tested without server connection)
        assertEquals("Failed to create protocol proxy!", true, proxy != null);
    }

    // Test code covering 'RPC.getProtocolProxy'
    @Test
    public void testRPC_getProtocolProxy() throws IOException {
        // 1. Create a Configuration object
        Configuration conf = new Configuration();

        // 2. Mock required input data
        InetSocketAddress addr = new InetSocketAddress("localhost", 8080);
        Class<TestProtocolImpl> protocol = TestProtocolImpl.class; // Use an actual implementation class for testing
        long clientVersion = 1L;
        UserGroupInformation ticket = UserGroupInformation.getCurrentUser();
        SocketFactory factory = SocketFactory.getDefault();

        // 3. Call the method with adjusted parameter structure
        ProtocolProxy<TestProtocolImpl> proxy = RPC.getProtocolProxy(
            protocol,
            clientVersion,
            addr,
            ticket,
            conf,
            factory,
            0, // The rpcTimeout value
            null // RetryPolicy is null for the test
        );

        // 4. Validate the output is not null (proxies cannot be fully tested without server connection)
        assertEquals("Failed to create protocol proxy!", true, proxy != null);
    }

    // Create a simple interface to act as a test protocol
    public interface TestProtocolImpl {
        // Define example methods here for the protocol if necessary.
    }
}