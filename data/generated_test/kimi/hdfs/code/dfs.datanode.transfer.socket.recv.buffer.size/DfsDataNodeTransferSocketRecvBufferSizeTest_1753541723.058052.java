package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.DNConf;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class DfsDataNodeTransferSocketRecvBufferSizeTest {

    private Configuration conf;

    @Before
    public void setUp() {
        conf = new Configuration();
        // Reset to default value
        conf.unset(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY);
    }

    @Test
    public void testTransferSocketRecvBufferSizeDefaultValue() {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // 2. Prepare the test conditions.
        // 3. Test code.
        // 4. Code after testing.
        
        // Test
        DNConf dnConf = new DNConf(conf);
        
        // Verify
        assertEquals(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_DEFAULT, 
                     dnConf.getTransferSocketRecvBufferSize());
    }

    @Test
    public void testTransferSocketRecvBufferSizeCustomValue() {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // 2. Prepare the test conditions.
        int customValue = 64 * 1024; // 64KB
        conf.setInt(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY, customValue);
        // 3. Test code.
        DNConf dnConf = new DNConf(conf);
        // 4. Code after testing.
        assertEquals(customValue, dnConf.getTransferSocketRecvBufferSize());
    }

    @Test
    public void testTransferSocketRecvBufferSizeZeroValue() {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // 2. Prepare the test conditions.
        int zeroValue = 0;
        conf.setInt(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY, zeroValue);
        // 3. Test code.
        DNConf dnConf = new DNConf(conf);
        // 4. Code after testing.
        assertEquals(zeroValue, dnConf.getTransferSocketRecvBufferSize());
    }

    @Test
    public void testTransferSocketRecvBufferSizeNegativeValue() {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // 2. Prepare the test conditions.
        int negativeValue = -1;
        conf.setInt(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY, negativeValue);
        // 3. Test code.
        DNConf dnConf = new DNConf(conf);
        // 4. Code after testing.
        assertEquals(negativeValue, dnConf.getTransferSocketRecvBufferSize());
    }
}