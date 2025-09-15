package org.apache.zookeeper.test;

import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.junit.Test;

import java.io.File;
import java.net.InetSocketAddress;

import static org.mockito.Mockito.*;

public class ZooKeeperServerTest {

    @Test
    // Test code to validate ZooKeeperServerMain.runFromConfig(ServerConfig) correctly utilizes the clientPortAddress configuration.
    // 1. Using the ZooKeeper 3.5.6 API to obtain and mock configuration values.
    // 2. Prepare the test conditions by mocking relevant classes and their methods.
    // 3. Verify the correct invocation and behavior of the configuration usage.
    // 4. Perform post-testing verification to ensure expected behavior.
    public void test_ZooKeeperServerMain_RunFromConfig_WithValidClientPortAddress() throws Exception {
        // Step 1: Mocking ServerConfig and providing sample configuration values
        ZooKeeperServerMain serverMain = new ZooKeeperServerMain();
        ServerConfig serverConfigMock = mock(ServerConfig.class);
        File mockDataDir = new File("mockDataDir");
        File mockDataLogDir = new File("mockDataLogDir");
        InetSocketAddress testClientPortAddress = new InetSocketAddress("127.0.0.1", 2181);
        int mockMaxClientConnections = 100;

        when(serverConfigMock.getDataDir()).thenReturn(mockDataDir);
        when(serverConfigMock.getDataLogDir()).thenReturn(mockDataLogDir);
        when(serverConfigMock.getClientPortAddress()).thenReturn(testClientPortAddress);
        when(serverConfigMock.getMaxClientCnxns()).thenReturn(mockMaxClientConnections);

        // Step 2: Mocking ServerCnxnFactory and its methods
        ServerCnxnFactory serverCnxnFactoryMock = mock(ServerCnxnFactory.class);
        doNothing().when(serverCnxnFactoryMock).configure(eq(testClientPortAddress), eq(mockMaxClientConnections), eq(false));
        doNothing().when(serverCnxnFactoryMock).startup(any());

        // Step 3: Invoke ZooKeeperServerMain.runFromConfig and verify behavior
        serverMain.runFromConfig(serverConfigMock);

        verify(serverConfigMock, times(1)).getDataDir();
        verify(serverConfigMock, times(1)).getDataLogDir();
        verify(serverConfigMock, times(1)).getClientPortAddress();
        verify(serverConfigMock, times(1)).getMaxClientCnxns();

        verify(serverCnxnFactoryMock, times(1)).configure(eq(testClientPortAddress), eq(mockMaxClientConnections), eq(false));
        verify(serverCnxnFactoryMock, times(1)).startup(any());
    }

    @Test
    // Test code to validate QuorumPeerMain.runFromConfig(QuorumPeerConfig) correctly utilizes the clientPortAddress configuration.
    // 1. Using ZooKeeper 3.5.6 API for proper mocking of configuration-related functions.
    // 2. Preparing test cases that simulate valid configuration values.
    // 3. Implementing verification for configuration propagation during QuorumPeer initialization.
    // 4. Performing steps to check correct binding to clientPortAddress.
    public void test_QuorumPeerMain_RunFromConfig_WithValidClientPortAddress() throws Exception {
        // Step 1: Mocking QuorumPeerConfig with sample configurations
        QuorumPeerMain quorumPeerMain = new QuorumPeerMain();
        QuorumPeerConfig configMock = mock(QuorumPeerConfig.class);
        File mockDataDir = new File("mockDataDir");
        File mockDataLogDir = new File("mockDataLogDir");
        InetSocketAddress testClientPortAddress = new InetSocketAddress("127.0.0.1", 2182);
        int mockMaxClientConnections = 50;

        when(configMock.getDataDir()).thenReturn(mockDataDir);
        when(configMock.getDataLogDir()).thenReturn(mockDataLogDir);
        when(configMock.getClientPortAddress()).thenReturn(testClientPortAddress);
        when(configMock.getMaxClientCnxns()).thenReturn(mockMaxClientConnections);

        // Mock ServerCnxnFactory
        ServerCnxnFactory cnxnFactoryMock = mock(ServerCnxnFactory.class);
        doNothing().when(cnxnFactoryMock).configure(eq(testClientPortAddress), eq(mockMaxClientConnections), eq(false));

        // Step 2: Call QuorumPeerMain.runFromConfig
        quorumPeerMain.runFromConfig(configMock);

        // Step 3: Verify configuration usage
        verify(configMock, times(1)).getDataDir();
        verify(configMock, times(1)).getDataLogDir();
        verify(configMock, times(1)).getClientPortAddress();
        verify(configMock, times(1)).getMaxClientCnxns();

        verify(cnxnFactoryMock, times(1)).configure(eq(testClientPortAddress), eq(mockMaxClientConnections), eq(false));
    }
}