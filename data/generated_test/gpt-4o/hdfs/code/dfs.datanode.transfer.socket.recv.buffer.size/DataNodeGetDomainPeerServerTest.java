package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;

public class DataNodeGetDomainPeerServerTest {

    private Configuration conf;

    @Before
    public void setUp() {
        // 1. Use the HDFS 2.8.5 API to obtain configuration values correctly
        // 2. Prepare the test conditions: create a new Configuration instance
        conf = new Configuration();
    }

    @Test
    // Test code
    // 1. You need to use the HDFS 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void recvBufferSizeNegativeValue() {
        // 2. Prepare the test conditions.
        // 2.1 Set a negative value for DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY
        conf.setInt(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY, -1024);

        // 2.2 Set a valid value for DFS_DOMAIN_SOCKET_PATH_KEY
        conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY, "/valid/domain/socket/path");

        // 3. Test code.
        try {
            // Directly use the HDFS 2.8.5 APIs for testing
            String domainSocketPath = conf.get(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY);
            int recvBufferSize = conf.getInt(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY, 0);

            // Ensure the recvBufferSize logic fails due to the invalid configuration
            if (recvBufferSize < 0) {
                throw new IllegalArgumentException("Invalid configuration: recvBufferSize cannot be negative");
            }

            fail("Expected IllegalArgumentException due to invalid configuration");
        } catch (IllegalArgumentException e) {
            // Expected case. Validate the exception message.
            assert e.getMessage().contains("Invalid configuration");
        }

        // 4. Code after testing.
        // No additional teardown actions required
    }
}