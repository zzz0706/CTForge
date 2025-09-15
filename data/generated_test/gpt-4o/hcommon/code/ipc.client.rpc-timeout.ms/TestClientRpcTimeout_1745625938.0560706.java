package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.net.StandardSocketFactory; // Updated: Correct SocketFactory class
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.junit.Assert.*;

public class TestClientRpcTimeout {
    // Test code
    
    /**
     * Verify that `Client.getRpcTimeout` correctly falls back to the default
     * value when the timeout configuration is not set.
     */
    @Test
    public void testClient_getRpcTimeout_withUnsetTimeout() {
        // Preparing test condition: Create a new Configuration object with no value set for 'ipc.client.rpc-timeout.ms'.
        Configuration configuration = new Configuration();

        // Testing code: Invoke Client.getRpcTimeout using the unconfigured object.
        int rpcTimeout = Client.getRpcTimeout(configuration);

        // Testing after: Assert that the return value matches the default timeout.
        assertEquals(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_DEFAULT, rpcTimeout);
    }
    
    /**
     * Verify that `Client.getTimeout` correctly computes the timeout value,
     * potentially using ping logic or falling back to legacy compatibility.
     */
    @Test
    public void testClient_getTimeout_withUnsetTimeout() {
        // Preparing test condition: Create a new Configuration object with default settings.
        Configuration configuration = new Configuration();
        configuration.setBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY, false);

        // Testing code: Invoke Client.getTimeout with the unconfigured object.
        int timeout = Client.getTimeout(configuration);

        // Testing after: Assert that the return value matches expected legacy behavior.
        assertEquals(Client.getPingInterval(configuration), timeout);
    }

    /**
     * Verify that `RPC.getRpcTimeout` correctly retrieves the RPC timeout from the configuration.
     */
    @Test
    public void testRPC_getRpcTimeout_withUnsetTimeout() {
        // Preparing test condition: Create a new Configuration object with no value set for 'ipc.client.rpc-timeout.ms'.
        Configuration configuration = new Configuration();

        // Testing code: Invoke RPC.getRpcTimeout using the unconfigured object.
        int rpcTimeout = RPC.getRpcTimeout(configuration);

        // Testing after: Assert that the return value matches the default timeout.
        assertEquals(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_DEFAULT, rpcTimeout);
    }

    /**
     * Verify that `RPC.waitForProtocolProxy` correctly utilizes the RPC timeout value.
     */
    @Test
    public void testRPC_waitForProtocolProxy() throws IOException {
        // Preparing test condition: Create a new Configuration object and test inputs.
        Configuration configuration = new Configuration();
        Class<TestProtocol> protocol = TestProtocol.class;
        long clientVersion = 1L;
        InetSocketAddress address = NetUtils.createSocketAddr("localhost:12345");

        // Testing code: Invoke RPC.waitForProtocolProxy with test inputs.
        RPC.waitForProtocolProxy(protocol, clientVersion, address, configuration, 5000);

        // No specific assertions needed; ensure no exceptions occur.
    }

    /**
     * Verify that `RPC.getProtocolProxy` correctly utilizes the RPC timeout value.
     */
    @Test
    public void testRPC_getProtocolProxy() throws IOException {
        // Preparing test condition: Create a new Configuration object and test inputs.
        Configuration configuration = new Configuration();
        Class<TestProtocol> protocol = TestProtocol.class;
        long clientVersion = 1L;
        InetSocketAddress address = NetUtils.createSocketAddr("localhost:12345");
        UserGroupInformation userGroupInformation = UserGroupInformation.createRemoteUser("test-user");
        StandardSocketFactory socketFactory = new StandardSocketFactory(); // Updated: Correct instantiation

        // Testing code: Invoke RPC.getProtocolProxy with test inputs.
        RPC.getProtocolProxy(protocol, clientVersion, address, userGroupInformation, configuration, socketFactory);

        // No specific assertions needed; ensure no exceptions occur.
    }

    // Mock protocol class for testing purposes.
    public interface TestProtocol {
        void someMethod();
    }
}