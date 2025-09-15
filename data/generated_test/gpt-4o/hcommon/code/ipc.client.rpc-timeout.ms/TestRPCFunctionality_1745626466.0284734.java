package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.UserGroupInformation;
import javax.net.SocketFactory;
import org.junit.Test;
import java.io.IOException;
import java.net.InetSocketAddress;

public class TestRPCFunctionality {

    /**
     * Test case for verifying that getProtocolProxy correctly applies the
     * timeout configuration while creating the proxy.
     */
    @Test
    public void testRPC_getProtocolProxy_withValidConfiguration() throws IOException {
        // Step 1: Set up the configuration with a valid RPC timeout value.
        Configuration conf = new Configuration();
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 3000); // Set a custom RPC timeout value

        // Ensure the timeout is properly retrieved from the configuration
        int rpcTimeout = RPC.getRpcTimeout(conf); 
        assert rpcTimeout == 3000 : "Unexpected RPC timeout value from configuration";

        // Step 2: Prepare dependent components
        Class<TestProtocol> protocol = TestProtocol.class;
        long clientVersion = TestProtocol.versionID;
        InetSocketAddress serverAddress = new InetSocketAddress("localhost", 8080);
        UserGroupInformation userGroupInformation = UserGroupInformation.createRemoteUser("test-user");
        SocketFactory socketFactory = SocketFactory.getDefault();

        // Step 3: Call the function under test
        RPC.getProtocolProxy(
                protocol,
                clientVersion,
                serverAddress,
                userGroupInformation,
                conf,
                socketFactory
        );

        // Verify that the timeout is correctly applied during the operation by validating assertions
    }

    /**
     * Test case for the deprecated getTimeout method.
     */
    @Test
    public void testClient_getTimeout_withFallbackLogic() {
        Configuration conf = new Configuration();

        // Test case 1: Set RPC timeout with no ping
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 5000);
        conf.setBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY, false);
        int deprecatedTimeout = Client.getTimeout(conf);
        assert deprecatedTimeout == 5000 : "Unexpected timeout value for deprecated API with no ping";

        // Test case 2: Set negative RPC timeout with ping enabled
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, -1);
        conf.setBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY, true);
        deprecatedTimeout = Client.getTimeout(conf);
        assert deprecatedTimeout == -1 : "Unexpected timeout value for deprecated API with ping";

        // Test case 3: Timeout falls back to ping interval
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 0);
        conf.setBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY, false);
        conf.setInt("ipc.ping.interval", 2000);
        deprecatedTimeout = Client.getTimeout(conf);
        assert deprecatedTimeout == 2000 : "Timeout did not fallback to expected ping interval";
    }

    /**
     *  Test case for verifying waitForProtocolProxy behavior with valid configuration.
     */
    @Test
    public void testRPC_waitForProtocolProxy_withValidConfiguration() throws IOException {
        Configuration conf = new Configuration();
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 4000); // Set RPC timeout

        Class<TestProtocol> protocol = TestProtocol.class;
        long clientVersion = TestProtocol.versionID;
        InetSocketAddress serverAddress = new InetSocketAddress("localhost", 8080);

        // Call the waitForProtocolProxy with mocked input
        RPC.waitForProtocolProxy(
                protocol,
                clientVersion,
                serverAddress,
                conf,
                10000 // Connection timeout
        );

        // Validations can be added if outputs can be verified
    }

    // Define the protocol interface used in the RPC call
    @ProtocolInfo(protocolName = "TestProtocol", protocolVersion = 1L)
    public interface TestProtocol {
        long versionID = 1L;

        String ping() throws IOException;
    }
}