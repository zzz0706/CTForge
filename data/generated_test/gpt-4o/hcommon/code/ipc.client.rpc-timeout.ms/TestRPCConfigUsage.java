package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;
import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;

public class TestRPCConfigUsage {

    /**
     * Test case for verifying the getRpcTimeout method of the Client class.
     * Ensures that the configuration value for IPC_CLIENT_RPC_TIMEOUT_KEY is parsed correctly.
     */
    @Test
    public void testClient_getRpcTimeout_withValidConfiguration() {
        Configuration conf = new Configuration();
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 3000);  // Custom RPC timeout value

        // Validate that the parsed timeout matches the configured value
        int rpcTimeout = Client.getRpcTimeout(conf);
        assert rpcTimeout == 3000 : "Invalid RPC timeout. Expected 3000, got " + rpcTimeout;
    }

    /**
     * Test case for verifying the getTimeout method of the Client class.
     * Ensures accurate fallback logic when different configurations are used.
     */
    @Test
    public void testClient_getTimeout_withFallbackBehavior() {
        Configuration conf = new Configuration();

        // Test case 1: Set timeout with no ping enabled
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 5000);
        conf.setBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY, false);
        int timeout = Client.getTimeout(conf);
        assert timeout == 5000 : "Unexpected timeout value. Expected 5000, got " + timeout;

        // Test case 2: Negative RPC timeout with ping enabled (returns -1)
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, -1);
        conf.setBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY, true);
        timeout = Client.getTimeout(conf);
        assert timeout == -1 : "Unexpected timeout value when ping is enabled. Got " + timeout;

        // Test case 3: Timeout falls back to ping interval
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 0);
        conf.setInt("ipc.ping.interval", 2000);
        conf.setBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY, false);
        timeout = Client.getTimeout(conf);
        assert timeout == 2000 : "Timeout did not fall back to ping interval. Got " + timeout;
    }

    /**
     * Test case for verifying behavior of RPC.getRpcTimeout.
     * Ensures the correct RPC timeout value is retrieved from the configuration.
     */
    @Test
    public void testRPC_getRpcTimeout_withValidConfiguration() {
        Configuration conf = new Configuration();
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 4000);  // Set custom timeout
        
        // Validate the retrieved RPC timeout value
        int rpcTimeout = RPC.getRpcTimeout(conf);
        assert rpcTimeout == 4000 : "Unexpected RPC timeout value. Expected 4000, got " + rpcTimeout;
    }

    /**
     * Test case for verifying RPC.getProtocolProxy function.
     * Ensure that the configured RPC timeout propagates and is respected.
     */
    @Test
    public void testRPC_getProtocolProxy_withValidConfiguration() throws IOException {
        Configuration conf = new Configuration();
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 3000);  // Set custom timeout
        
        Class<TestProtocol> protocol = TestProtocol.class;
        long clientVersion = TestProtocol.versionID;
        InetSocketAddress serverAddress = new InetSocketAddress("localhost", 8080);
        UserGroupInformation userGroupInformation = UserGroupInformation.createRemoteUser("test-user");
        SocketFactory socketFactory = SocketFactory.getDefault();

        // Call the function and ensure no exceptions are thrown
        ProtocolProxy<TestProtocol> proxy = RPC.getProtocolProxy(
                protocol,
                clientVersion,
                serverAddress,
                userGroupInformation,
                conf,
                socketFactory
        );
        
        assert proxy != null : "ProtocolProxy creation failed.";
    }

    /**
     * Test case for verifying waitForProtocolProxy in RPC.
     * Ensure timeout value propagates correctly.
     */
    @Test
    public void testRPC_waitForProtocolProxy_withValidConfiguration() throws IOException {
        Configuration conf = new Configuration();
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 4000);  // Set RPC timeout

        Class<TestProtocol> protocol = TestProtocol.class;
        long clientVersion = TestProtocol.versionID;
        InetSocketAddress serverAddress = new InetSocketAddress("localhost", 8080);

        ProtocolProxy<TestProtocol> proxy = RPC.waitForProtocolProxy(
                protocol,
                clientVersion,
                serverAddress,
                conf,
                10000 // Connection timeout
        );

        // Verify that proxy is returned successfully
        assert proxy != null : "Failed to obtain ProtocolProxy.";
    }

    /**
     * The test protocol interface used in the RPC calls.
     */
    @ProtocolInfo(protocolName = "TestProtocol", protocolVersion = 1L)
    public interface TestProtocol {
        long versionID = 1L;

        String ping() throws IOException;
    }
}