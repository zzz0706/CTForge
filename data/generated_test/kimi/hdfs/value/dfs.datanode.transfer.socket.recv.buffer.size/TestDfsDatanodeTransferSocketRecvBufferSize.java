package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestDfsDatanodeTransferSocketRecvBufferSize {

    private Configuration conf;

    @Before
    public void setUp() {
        // Prepare the test conditions: create a fresh Configuration instance
        conf = new Configuration();
    }

    @Test
    public void testRecvBufferSizeValidRange() {
        // 1. Read the configuration value from the configuration file (no hard-coding)
        int recvBufferSize = conf.getInt(
                DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY,
                DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_DEFAULT);

        // 2. Validate the configuration value
        // According to the description, any value <= 0 is acceptable (enables TCP auto-tuning)
        assertTrue("dfs.datanode.transfer.socket.recv.buffer.size must be >= 0",
                   recvBufferSize >= 0);
    }

    @After
    public void tearDown() {
        // 4. Code after testing: clean-up if necessary
        conf = null;
    }
}