package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.net.NetUtils;
import org.junit.Test;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

public class TestDFSUtil {

    @Test
    // Test code
    // 1. Use the HDFS 2.8.5 API correctly to obtain configuration values instead of hardcoding them.
    // 2. Prepare the test conditions.
    // 3. Execute the test code.
    // 4. Validate the results.
    public void testGetNNServiceRpcAddressesForClusterWithInvalidInternalNameservices() {
        // 1. Prepare the test conditions
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "undefined_ns"); // Undefined nameservice
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2"); // Valid nameservices
        
        try {
            // 2. Test code
            // Attempt to fetch NN Service RPC Addresses for the cluster
            DFSUtil.getNNServiceRpcAddressesForCluster(conf);
        } catch (IOException e) {
            // 3. Validate the exception
            String exceptionMessage = e.getMessage();
            assert exceptionMessage.contains("Unknown nameservice: undefined_ns") : 
                "Expected exception containing 'Unknown nameservice: undefined_ns', but got: " + exceptionMessage;
            return; 
        }

        // 4. Fail the test if no exception was thrown
        assert false : "Expected IOException was not thrown.";
    }

    @Test
    // Test code
    // 1. Use the HDFS 2.8.5 API correctly to obtain configuration values instead of hardcoding them.
    // 2. Prepare the test conditions.
    // 3. Execute the test code.
    // 4. Validate the results.
    public void testGetNNLifelineRpcAddressesForClusterWithInvalidInternalNameservices() {
        // 1. Prepare the test conditions
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "undefined_ns"); // Undefined nameservice
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2"); // Valid nameservices
        
        try {
            // 2. Test code
            // Attempt to fetch NN Lifeline RPC Addresses for the cluster
            DFSUtil.getNNLifelineRpcAddressesForCluster(conf);
        } catch (IOException e) {
            // 3. Validate the exception
            String exceptionMessage = e.getMessage();
            assert exceptionMessage.contains("Unknown nameservice: undefined_ns") : 
                "Expected exception containing 'Unknown nameservice: undefined_ns', but got: " + exceptionMessage;
            return; 
        }

        // 4. Fail the test if no exception was thrown
        assert false : "Expected IOException was not thrown.";
    }
}