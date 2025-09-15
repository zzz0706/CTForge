package org.apache.zookeeper.test;

import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.ServerConfig;
import org.junit.Test;

import java.net.InetSocketAddress;

import static org.mockito.Mockito.*;
import static org.junit.Assert.assertEquals;

public class ZooKeeperServerTest {

    @Test
    //test code
    // 1. You need to use the zookeeper3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testGetSecureClientPortWithSecureServerFactoryPresent() {
        // Step 1: Set up conditions and mocks
        // Mocking ServerConfig to simulate configuration retrieval
        ServerConfig serverConfigMock = mock(ServerConfig.class);
        ZooKeeperServerMain zooKeeperServerMain = new ZooKeeperServerMain();
        InetSocketAddress secureClientPortAddress = new InetSocketAddress("127.0.0.1", 2281);

        // Mocking configuration retrieval
        when(serverConfigMock.getSecureClientPortAddress()).thenReturn(secureClientPortAddress);

        // Preparing a mocked ZooKeeperServer instance
        ServerCnxnFactory secureServerCnxnFactoryMock = mock(ServerCnxnFactory.class);
        ZooKeeperServer zooKeeperServer = spy(new ZooKeeperServer());

        // Simulating the secureServerCnxnFactory setup
        doReturn(secureServerCnxnFactoryMock).when(zooKeeperServer).getSecureServerCnxnFactory();
        when(secureServerCnxnFactoryMock.getLocalPort()).thenReturn(secureClientPortAddress.getPort());

        try {
            // Step 2: Run the main server configuration to see if it properly initializes secure factory
            zooKeeperServerMain.runFromConfig(serverConfigMock);

            // Step 3: Call the API to get secure client port based on configuration
            int secureClientPort = zooKeeperServer.getSecureClientPort();

            // Step 4: Validate the result
            assertEquals("Expected secure client port should match the configured port",
                    secureClientPortAddress.getPort(), secureClientPort);

        } catch (Exception e) {
            e.printStackTrace();
            // Fail test in case of unexpected exception
            throw new RuntimeException("Test failed due to exception: " + e.getMessage());
        } finally {
            // Step 5: Clean up after testing
            secureServerCnxnFactoryMock = null;
            zooKeeperServer = null;
        }
    }
}