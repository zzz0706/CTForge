package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.DNConf;
import org.apache.hadoop.net.unix.DomainSocket;
import org.junit.*;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.powermock.api.mockito.PowerMockito.*;
import static org.junit.Assert.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DomainSocket.class})
public class DfsDataNodeTransferSocketRecvBufferSizeTest {

    private Configuration conf;

    @Before
    public void setUp() throws Exception {
        // Initialize configuration
        conf = new Configuration(false);
    }

    @Test
    public void testTransferSocketRecvBufferSize_DefaultValue() throws Exception {
        // Prepare
        int defaultValue = DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_DEFAULT;
        
        // Mock static methods
        PowerMockito.mockStatic(DomainSocket.class);
        PowerMockito.when(DomainSocket.getLoadingFailureReason()).thenReturn(null);

        // Execute
        DNConf dnConf = new DNConf(conf);
        int actualValue = dnConf.getTransferSocketRecvBufferSize();

        // Verify
        assertEquals("Default transfer socket receive buffer size should match", 
                           defaultValue, actualValue);
    }

    @Test
    public void testTransferSocketRecvBufferSize_CustomValue_Positive() throws Exception {
        // Prepare
        int customValue = 1024 * 1024; // 1MB
        conf.setInt(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY, customValue);

        // Mock static methods
        PowerMockito.mockStatic(DomainSocket.class);
        PowerMockito.when(DomainSocket.getLoadingFailureReason()).thenReturn(null);

        // Execute
        DNConf dnConf = new DNConf(conf);
        int actualValue = dnConf.getTransferSocketRecvBufferSize();

        // Verify
        assertEquals("Custom transfer socket receive buffer size should match", 
                           customValue, actualValue);
    }

    @Test
    public void testTransferSocketRecvBufferSize_CustomValue_Zero() throws Exception {
        // Prepare
        int customValue = 0;
        conf.setInt(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY, customValue);

        // Mock static methods
        PowerMockito.mockStatic(DomainSocket.class);
        PowerMockito.when(DomainSocket.getLoadingFailureReason()).thenReturn(null);

        // Execute
        DNConf dnConf = new DNConf(conf);
        int actualValue = dnConf.getTransferSocketRecvBufferSize();

        // Verify
        assertEquals("Zero transfer socket receive buffer size should match", 
                           customValue, actualValue);
    }

    @Test
    public void testTransferSocketRecvBufferSize_CustomValue_Negative() throws Exception {
        // Prepare
        int customValue = -1;
        conf.setInt(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY, customValue);

        // Mock static methods
        PowerMockito.mockStatic(DomainSocket.class);
        PowerMockito.when(DomainSocket.getLoadingFailureReason()).thenReturn(null);

        // Execute
        DNConf dnConf = new DNConf(conf);
        int actualValue = dnConf.getTransferSocketRecvBufferSize();

        // Verify
        assertEquals("Negative transfer socket receive buffer size should match", 
                           customValue, actualValue);
    }

    @Test
    public void testConfigurationValue_MatchesConfigurationGet() throws Exception {
        // Prepare
        int customValue = 1024 * 1024; // 1MB
        conf.setInt(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY, customValue);

        // Get value via Configuration
        int configValue = conf.getInt(
            DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY,
            DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_DEFAULT);

        // Verify
        assertEquals("Configuration value should match set value",
                           customValue, configValue);
    }
}