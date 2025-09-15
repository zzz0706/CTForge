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
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testGetNNServiceRpcAddressesForCluster_withInvalidInternalNameservices() {
        // 1. Prepare the test conditions
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "undefined_ns"); // Undefined nameservice
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2"); // Valid nameservices

        try {
            // 2. Test code
            // Attempt to fetch NameNode Service RPC addresses
            Map<String, Map<String, InetSocketAddress>> addresses = DFSUtil.getNNServiceRpcAddressesForCluster(conf);
            
            // If no exception was thrown, fail the test
            assert false : "Expected IOException was not thrown.";
        } catch (IOException e) {
            // Validate the exception
            String exceptionMessage = e.getMessage();
            assert exceptionMessage.contains("Unknown nameservice: undefined_ns") : 
                "Expected exception containing 'Unknown nameservice: undefined_ns', but got: " + exceptionMessage;
        }
    }

    @Test
    // Test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testGetNNLifelineRpcAddressesForCluster_withInvalidInternalNameservices() {
        // 1. Prepare the test conditions
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "undefined_ns"); // Undefined nameservice
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2"); // Valid nameservices

        try {
            // 2. Test code
            // Attempt to fetch NameNode Lifeline RPC addresses
            Map<String, Map<String, InetSocketAddress>> lifelineAddresses = DFSUtil.getNNLifelineRpcAddressesForCluster(conf);
            
            // If no exception was thrown, fail the test
            assert false : "Expected IOException was not thrown.";
        } catch (IOException e) {
            // Validate the exception
            String exceptionMessage = e.getMessage();
            assert exceptionMessage.contains("Unknown nameservice: undefined_ns") : 
                "Expected exception containing 'Unknown nameservice: undefined_ns', but got: " + exceptionMessage;
        }
    }
}