package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.net.NetUtils;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TestDataNodeConfiguration {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getDataPort_validConfiguration() throws Exception {
        // Step 1: Prepare the test conditions
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY, "127.0.0.1:50010");
        conf.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, "127.0.0.1:9000"); // Fix: Configure NameNode RPC address

        String dataNodeAddress = conf.getTrimmed(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY, DFSConfigKeys.DFS_DATANODE_ADDRESS_DEFAULT);

        // Step 2: Start DataNode
        DataNode dataNodeInstance = DataNode.instantiateDataNode(new String[]{}, conf);

        // Step 3: Test getDataPort functionality
        int returnedPort = NetUtils.createSocketAddr(conf.get(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY)).getPort(); // Fix: Simplify access to DataNode configuration

        // Step 4: Verify the expected result
        int expectedPort = NetUtils.createSocketAddr(dataNodeAddress).getPort();
        assertEquals("Returned port must match the port configured in DFS_DATANODE_ADDRESS_KEY.", expectedPort, returnedPort);

        // Clean up after testing
        if (dataNodeInstance != null) {
            dataNodeInstance.shutdown();
        }
    }
}