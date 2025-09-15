package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.net.NetUtils;
import org.junit.Test;

import java.net.InetSocketAddress;

public class TestDataNodeConfiguration {

    @Test
    // test_getStreamingAddr_DefaultAddress
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getStreamingAddr_DefaultAddress() {
        // 1. Prepare the test conditions
        // Create a Configuration instance without setting the dfs.datanode.address property.
        Configuration conf = new Configuration();

        // 2. Test code: Call the method
        InetSocketAddress resultAddr = DataNode.getStreamingAddr(conf);

        // 3. Assert the expected result
        // Verify that the returned InetSocketAddress matches the default address and port (0.0.0.0:50010).
        InetSocketAddress expectedAddr = NetUtils.createSocketAddr(
            conf.getTrimmed(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY, DFSConfigKeys.DFS_DATANODE_ADDRESS_DEFAULT));
        assert resultAddr.equals(expectedAddr) : "The InetSocketAddress does not match the expected default address.";

        // 4. Code after testing (optional cleanup or additional verification)
        System.out.println("Test passed with address: " + resultAddr);
    }
}