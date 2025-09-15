package org.apache.zookeeper.test;

import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.junit.Test;

import java.io.File;
import java.net.InetSocketAddress;

import static org.mockito.Mockito.*;

public class ZooKeeperServerMainTest {

    @Test
    // Test code to validate that ZooKeeperServerMain.runFromConfig(ServerConfig) correctly utilizes 
    // the clientPortAddress configuration and binds to the expected address.
    // 1. You need to use the zookeeper3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_ZooKeeperServerMain_RunFromConfig_WithValidClientPortAddress() throws Exception {
        // Step 1: Set up the environment
        ZooKeeperServerMain serverMain = new ZooKeeperServerMain();
        ServerConfig serverConfigMock = mock(ServerConfig.class);
        File mockDataDir = new File("mockDataDir");
        File mockDataLogDir = new File("mockDataLogDir");
        InetSocketAddress testClientPortAddress = new InetSocketAddress("127.0.0.1", 2181);
        int mockMaxClientConnections = 100;

        // Mock the ServerConfig methods to return desired configuration values for tests
        when(serverConfigMock.getDataDir()).thenReturn(mockDataDir);
        when(serverConfigMock.getDataLogDir()).thenReturn(mockDataLogDir);
        when(serverConfigMock.getClientPortAddress()).thenReturn(testClientPortAddress);
        when(serverConfigMock.getMaxClientCnxns()).thenReturn(mockMaxClientConnections);

        // Step 2: Create a mock for ServerCnxnFactory instead of using mockConstruction
        ServerCnxnFactory serverCnxnFactoryMock = mock(ServerCnxnFactory.class);

        // Mock the configure method
        doNothing().when(serverCnxnFactoryMock).configure(eq(testClientPortAddress), eq(mockMaxClientConnections), eq(false));
        // Mock the startup method
        doNothing().when(serverCnxnFactoryMock).startup(any());

        // Step 3: Invocation of ZooKeeperServerMain.runFromConfig
        serverMain.runFromConfig(serverConfigMock);

        // Step 4: Verification of behaviors
        // Verify that the ServerConfig getters were called
        verify(serverConfigMock, times(1)).getDataDir();
        verify(serverConfigMock, times(1)).getDataLogDir();
        verify(serverConfigMock, times(1)).getClientPortAddress();
        verify(serverConfigMock, times(1)).getMaxClientCnxns();

        // Verify interactions with ServerCnxnFactory's configure and startup methods
        verify(serverCnxnFactoryMock, times(1)).configure(eq(testClientPortAddress), eq(mockMaxClientConnections), eq(false));
        verify(serverCnxnFactoryMock, times(1)).startup(any());
    }
}