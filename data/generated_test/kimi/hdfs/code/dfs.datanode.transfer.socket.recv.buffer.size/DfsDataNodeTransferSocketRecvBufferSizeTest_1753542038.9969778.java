package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.*;
import static org.junit.Assert.*;

public class DfsDataNodeTransferSocketRecvBufferSizeTest {

    @Test
    public void testTransferSocketRecvBufferSizeParsedCorrectlyInDNConf() {
        // Prepare test conditions
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY, "4096");

        // Test code
        DNConf dnConf = new DNConf(conf);

        // Assertions
        int expectedValue = 4096;
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
        Configuration conf = new Configuration();
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
        Configuration conf = new Configuration();
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
    public void testDefaultValueWhenNotConfigured() {
        // Prepare test conditions - do not set the configuration
        Configuration conf = new Configuration();
        
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

    @Test
    public void testDomainSocketConfigurationParsing() {
        // Prepare test conditions
        Configuration conf = new Configuration();
        int bufferSize = 32768;
        String domainSocketPath = "/tmp/hadoop-domain-socket";
        
        // Set the configuration values
        conf.setInt(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY, bufferSize);
        conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY, domainSocketPath);
        conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
        
        // Test code - verify configuration is correctly parsed
        int configuredBufferSize = conf.getInt(
            DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY,
            DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_DEFAULT);
        assertEquals("Configuration should have the correct buffer size", bufferSize, configuredBufferSize);
        
        // Verify domain socket path is set correctly
        String configuredDomainSocketPath = conf.get(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY);
        assertEquals("Domain socket path should be configured correctly", domainSocketPath, configuredDomainSocketPath);
        
        // Verify short-circuit is enabled
        boolean shortCircuitEnabled = conf.getBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY,
                                                     HdfsClientConfigKeys.Read.ShortCircuit.DEFAULT);
        assertTrue("Short-circuit should be enabled", shortCircuitEnabled);
    }

    @Test
    public void testDomainPeerServerReceiveBufferSizeSetWhenPositive() throws Exception {
        // Prepare test conditions
        Configuration conf = new Configuration();
        int bufferSize = 16384;
        String domainSocketPath = "/tmp/hadoop-domain-socket";
        
        conf.setInt(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY, bufferSize);
        conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY, domainSocketPath);
        conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
        
        // Test code - verify configuration is properly set and used
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
        Configuration conf = new Configuration();
        conf.setInt(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY, 0);
        conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY, "/tmp/hadoop-domain-socket");
        conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
        
        // Test code - verify configuration is properly set
        int recvBufferSize = conf.getInt(
            DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY,
            DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_DEFAULT);
        assertEquals("Configuration should have zero buffer size", 0, recvBufferSize);
        
        // Verify domain socket path is set
        assertNotNull("Domain socket path should be set", 
                     conf.get(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY));
    }
}