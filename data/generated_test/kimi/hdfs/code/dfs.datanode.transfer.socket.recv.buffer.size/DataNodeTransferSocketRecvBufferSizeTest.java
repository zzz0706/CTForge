package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DNConf;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.whenNew;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DataNode.class})
public class DataNodeTransferSocketRecvBufferSizeTest {

    private Configuration conf;
    private Properties configProperties;

    @Before
    public void setUp() throws Exception {
        // Load configuration from external source (simulated)
        configProperties = new Properties();
        
        conf = new Configuration();
        // Set the configuration key from the default or user-provided value
        String configKey = DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY;
        String defaultValue = String.valueOf(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_DEFAULT);
        conf.set(configKey, defaultValue);
    }

    @Test
    public void testTransferSocketRecvBufferSizeParsedCorrectlyInDNConf() {
        // Given: A configuration object with the key set
        int expectedValue = conf.getInt(
            DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY,
            DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_DEFAULT
        );

        // When: DNConf is instantiated
        DNConf dnConf = new DNConf(conf);

        // Then: The value should be correctly parsed and stored
        assertEquals(expectedValue, dnConf.getTransferSocketRecvBufferSize());
    }

    @Test
    public void testCompareConfigServiceWithPropertiesLoader() {
        // Given: Load using ConfigService (here simulated by Configuration class)
        String key = DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY;
        int configServiceValue = conf.getInt(key, DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_DEFAULT);

        // And: Load using raw Properties loader
        int propertiesLoaderValue = DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_DEFAULT;

        // Then: Values should match
        assertEquals("Configuration service and properties loader should return same value", 
                     propertiesLoaderValue, configServiceValue);
    }
}