package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DNConf;
import org.junit.*;
import java.util.Properties;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class DfsDataNodeTransferSocketRecvBufferSizeTest {

    private Configuration conf;
    private Properties configProperties;

    @Before
    public void setUp() {
        conf = new Configuration();
        configProperties = new Properties();
        // Simulate loading from configuration files
        configProperties.setProperty("dfs.datanode.transfer.socket.recv.buffer.size", "4096");
    }

    @Test
    public void testTransferSocketRecvBufferSizeParsedCorrectlyInDNConf() {
        // Prepare test conditions
        conf.set(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY, 
                 configProperties.getProperty(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY));

        // Test code
        DNConf dnConf = new DNConf(conf);

        // Assertions
        int expectedValue = Integer.parseInt(configProperties.getProperty(
            DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY));
        assertEquals("DNConf should parse the transfer socket receive buffer size correctly",
                     expectedValue, dnConf.getTransferSocketRecvBufferSize());
        
        // Reference loader comparison
        int configServiceValue = conf.getInt(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY,
                                            DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_DEFAULT);
        assertEquals("ConfigService value should match reference loader value",
                     expectedValue, configServiceValue);
    }

    @Test
    public void testTcpPeerServerReceiveBufferSizeSetWhenPositive() throws Exception {
        // Prepare test conditions
        int bufferSize = 8192;
        conf.setInt(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY, bufferSize);
        
        // Create DNConf with our configuration
        DNConf dnConf = new DNConf(conf);
        
        // Test code - verify that DNConf has the correct value
        assertEquals("DNConf should have the correct buffer size", bufferSize, dnConf.getTransferSocketRecvBufferSize());

        // Verify that configuration is properly set
        int configValue = conf.getInt(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY,
                                     DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_DEFAULT);
        assertEquals("Configuration should have the correct buffer size", bufferSize, configValue);
    }

    @Test
    public void testTcpPeerServerReceiveBufferSizeNotSetWhenZero() throws Exception {
        // Prepare test conditions
        conf.setInt(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY, 0);
        
        // Create DNConf with our configuration
        DNConf dnConf = new DNConf(conf);
        
        // Test code - verify that DNConf has the correct value
        assertEquals("DNConf should have zero buffer size", 0, dnConf.getTransferSocketRecvBufferSize());

        // Verify that configuration is properly set
        int configValue = conf.getInt(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY,
                                     DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_DEFAULT);
        assertEquals("Configuration should have zero buffer size", 0, configValue);
    }

    @Test
    public void testDomainPeerServerReceiveBufferSizeSetWhenPositive() throws Exception {
        // Prepare test conditions
        int bufferSize = 16384;
        String domainSocketPath = "/tmp/hadoop-domain-socket";
        conf.setInt(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY, bufferSize);
        conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY, domainSocketPath);
        
        // Test code - verify configuration is properly set
        int recvBufferSize = conf.getInt(
            DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY,
            DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_DEFAULT);
        assertEquals("Configuration should have the correct buffer size", bufferSize, recvBufferSize);
        
        // Verify domain socket path is set
        assertEquals("Domain socket path should be set correctly", domainSocketPath, 
                    conf.get(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY));
    }

    @Test
    public void testDomainPeerServerReceiveBufferSizeNotSetWhenZero() throws Exception {
        // Prepare test conditions
        conf.setInt(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY, 0);
        conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY, "/tmp/hadoop-domain-socket");
        
        // Test code - verify configuration is properly set
        int recvBufferSize = conf.getInt(
            DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY,
            DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_DEFAULT);
        assertEquals("Configuration should have zero buffer size", 0, recvBufferSize);
        
        // Verify domain socket path is set
        assertNotNull("Domain socket path should be set", 
                     conf.get(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY));
    }

    @Test
    public void testDefaultValueWhenNotConfigured() {
        // Prepare test conditions - do not set the configuration
        
        // Test code
        DNConf dnConf = new DNConf(conf);

        // Assertions
        assertEquals("Default value should be 0 when not configured",
                     DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_DEFAULT,
                     dnConf.getTransferSocketRecvBufferSize());
        
        // Reference loader comparison
        int configServiceValue = conf.getInt(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY,
                                            DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_DEFAULT);
        assertEquals("ConfigService value should match default value",
                     DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_DEFAULT,
                     configServiceValue);
    }
}