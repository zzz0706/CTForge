package org.apache.hadoop.hdfs;   

import org.apache.hadoop.conf.Configuration;       
import org.apache.hadoop.hdfs.DFSUtil;
import java.net.InetSocketAddress;
import java.util.Map;

import org.junit.Test;
import static org.junit.Assert.assertNotNull;

public class TestDFSUtil {   

    @Test 
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getNNLifelineRpcAddressesForCluster_withValidInternalNameservices() throws Exception {
        // Prepare the test conditions: create a Configuration instance
        Configuration conf = new Configuration();

        // Set up valid dfs.internal.nameservices and dfs.nameservices configuration
        conf.set("dfs.nameservices", "ns1,ns2");
        conf.set("dfs.internal.nameservices", "ns1,ns2");
        conf.set("dfs.namenode.lifeline.rpc-address.ns1", "localhost:8021");
        conf.set("dfs.namenode.lifeline.rpc-address.ns2", "localhost:8022");

        // Test code: call getNNLifelineRpcAddressesForCluster()
        Map<String, Map<String, InetSocketAddress>> lifelineRpcAddresses = DFSUtil.getNNLifelineRpcAddressesForCluster(conf);

        // Assertions: ensure the function returns the expected map
        assertNotNull("Lifeline RPC addresses map should not be null", lifelineRpcAddresses);

        Map<String, InetSocketAddress> ns1Addresses = lifelineRpcAddresses.get("ns1");
        Map<String, InetSocketAddress> ns2Addresses = lifelineRpcAddresses.get("ns2");

        assertNotNull("Lifeline RPC addresses for 'ns1' should not be null", ns1Addresses);
        assertNotNull("Lifeline RPC addresses for 'ns2' should not be null", ns2Addresses);

        // Print values for debugging (optional, but shouldn't replace assertions)
        System.out.println("ns1 Lifeline RPC Address: " + ns1Addresses);
        System.out.println("ns2 Lifeline RPC Address: " + ns2Addresses);
    }
}