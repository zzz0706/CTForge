package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class DfsDataNodeTransferSocketRecvBufferSizeTest {

    private Configuration conf;
    private Properties configProperties;

    @Before
    public void setUp() {
        conf = new Configuration();
        configProperties = new Properties();
        // Load default configuration values
        configProperties.setProperty(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY, 
                                    String.valueOf(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_DEFAULT));
    }

    @Test
    public void testTransferSocketRecvBufferSizeDefaultValue() {
        // Prepare
        int expectedValue = Integer.parseInt(configProperties.getProperty(
                DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY));
        
        // Test
        DNConf dnConf = new DNConf(conf);
        
        // Verify
        assertEquals(expectedValue, dnConf.getTransferSocketRecvBufferSize());
    }

    @Test
    public void testTransferSocketRecvBufferSizeCustomValue() {
        // Prepare
        int customValue = 64 * 1024; // 64KB
        conf.setInt(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY, customValue);
        configProperties.setProperty(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY, 
                                    String.valueOf(customValue));
        
        // Test
        DNConf dnConf = new DNConf(conf);
        
        // Verify
        assertEquals(customValue, dnConf.getTransferSocketRecvBufferSize());
        
        // Also verify against configuration file value
        int configValue = Integer.parseInt(configProperties.getProperty(
                DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY));
        assertEquals(configValue, dnConf.getTransferSocketRecvBufferSize());
    }

    @Test
    public void testTransferSocketRecvBufferSizeZeroValue() {
        // Prepare
        int zeroValue = 0;
        conf.setInt(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY, zeroValue);
        
        // Test
        DNConf dnConf = new DNConf(conf);
        
        // Verify
        assertEquals(zeroValue, dnConf.getTransferSocketRecvBufferSize());
    }

    @Test
    public void testTransferSocketRecvBufferSizeNegativeValue() {
        // Prepare
        int negativeValue = -1;
        conf.setInt(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY, negativeValue);
        
        // Test
        DNConf dnConf = new DNConf(conf);
        
        // Verify
        assertEquals(negativeValue, dnConf.getTransferSocketRecvBufferSize());
    }
}