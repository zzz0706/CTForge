package org.apache.hadoop.hdfs.server.datanode;   

import org.apache.hadoop.conf.Configuration;       
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.DNConf;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class TestDataNodeInitialization {   

    @Test 
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_initDataXceiver_socket_buffer_size() throws IOException {
        // Step 1: Prepare test conditions
        // Create a Configuration object with default settings
        Configuration conf = new Configuration();
        
        // Retrieve configuration value using the correct API
        int bufferSize = conf.getInt(
            DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY,
            DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_DEFAULT
        );
        
        // Ensure a valid buffer size is set
        assertTrue("Buffer size should be greater than or equal to 0", bufferSize >= 0);

        // Step 2: Test code
        // Instantiate a DNConf using the Configuration
        DNConf dnConf = new DNConf(conf);

        // Verify the buffer size is correctly applied in DNConf
        assertTrue("DNConf should have the configured buffer size",
            dnConf.getTransferSocketRecvBufferSize() == bufferSize
        );

        // Step 3: Code after testing
        // Clean up/release allocated resources (if necessary)
        // For this test, there is no explicit resource cleanup needed.
    }
}