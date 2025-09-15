package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;
import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;

public class TestRPCConfigurationUsage {
    
    /**
     * Test case for verifying the usage of getRpcTimeout in Client.
     * Ensures the configuration value for IPC_CLIENT_RPC_TIMEOUT_KEY is retrieved correctly.
     */
    @Test
    public void testClient_getRpcTimeout_withValidConfiguration() {
        Configuration conf = new Configuration();
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 3000);  // Set custom timeout value
        
        // Validate that the parsed timeout matches the configured value
        int rpcTimeout = Client.getRpcTimeout(conf);
        assert rpcTimeout == 3000 : "Invalid RPC timeout. Expected 3000, got " + rpcTimeout;
    }

    /**
     * Test case for verifying the deprecated getTimeout method in Client.
     * Ensures fallback behavior is handled correctly in various scenarios.
     */
    @Test
    public void testClient_getTimeout_withFallbackLogic() {
        Configuration conf = new Configuration();

        // Test case 1: Timeout directly set and ping disabled
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 5000);
        conf.setBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY, false);
        int timeout = Client.getTimeout(conf);
        assert timeout == 5000 : "Timeout should match directly set value. Expected 5000, got " + timeout;

        // Test case 2: Timeout negative and ping enabled
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, -1);
        conf.setBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY, true);
        timeout = Client.getTimeout(conf);
        assert timeout == -1 : "Timeout should be -1 when negative value is set and ping is enabled. Got " + timeout;

        // Test case 3: Timeout falls back to ping interval
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 0);
        conf.setInt("ipc.ping.interval", 2000);
        conf.setBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY, false);
        timeout = Client.getTimeout(conf);
        assert timeout == 2000 : "Timeout fallback to ping interval failed. Expected 2000, got " + timeout;
    }

    /**
     * Test case for verifying getRpcTimeout method in RPC.
     * Ensures the configuration value is retrieved and used appropriately.
     */
    @Test
    public void testRPC_getRpcTimeout_withValidConfiguration() {
        Configuration conf = new Configuration();
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 4000);  // Set custom value
        
        // Validate retrieved timeout value
        int rpcTimeout = RPC.getRpcTimeout(conf);
        assert rpcTimeout == 4000 : "Unexpected RPC timeout. Expected 4000, got " + rpcTimeout;
    }

    /**
     * Test case for verifying the getProtocolProxy functionality in RPC.
     * Ensures the RPC timeout configuration is propagated correctly.
     */
    @Test
    public void testRPC_getProtocolProxy_withConfigurationUsage() throws IOException {
        Configuration conf = new Configuration();
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 3000);  // Custom timeout
        
        Class<TestProtocol> protocol = TestProtocol.class;
        long clientVersion = TestProtocol.versionID;
        InetSocketAddress serverAddress = new InetSocketAddress("localhost", 8080);
        UserGroupInformation user = UserGroupInformation.createRemoteUser("test-user");
        SocketFactory factory = SocketFactory.getDefault();

        ProtocolProxy<TestProtocol> proxy = RPC.getProtocolProxy(
            protocol,
            clientVersion,
            serverAddress,
            user,
            conf,
            factory
        );

        assert proxy != null : "ProtocolProxy creation failed.";
    }

    /**
     * Test case for verifying the waitForProtocolProxy functionality in RPC.
     * Ensures correct handling of configuration-provided timeouts.
     */
    @Test
    public void testRPC_waitForProtocolProxy_withConfigurationUsage() throws IOException {
        Configuration conf = new Configuration();
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 4000);  // Set timeout
        
        Class<TestProtocol> protocol = TestProtocol.class;
        long clientVersion = TestProtocol.versionID;
        InetSocketAddress addr = new InetSocketAddress("localhost", 8080);

        ProtocolProxy<TestProtocol> proxy = RPC.waitForProtocolProxy(
            protocol,
            clientVersion,
            addr,
            conf,
            10000 // Connection timeout
        );

        assert proxy != null : "Failed to retrieve ProtocolProxy.";
    }

    /**
     * Sample protocol for test purposes.
     */
    @ProtocolInfo(protocolName = "TestProtocol", protocolVersion = 1L)
    public interface TestProtocol {
        long versionID = 1L;

        String ping() throws IOException;
    }
}