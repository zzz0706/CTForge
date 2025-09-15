package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.net.NetUtils;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestDFSUtil {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getNNServiceRpcAddressesForCluster_withValidInternalNameservices() throws Exception {
        // Prepare the test conditions: create a Configuration instance
        Configuration conf = new Configuration();

        // Set up valid dfs.internal.nameservices and dfs.nameservices configuration
        conf.set("dfs.nameservices", "ns1,ns2");
        conf.set("dfs.internal.nameservices", "ns1,ns2");
        conf.set("dfs.namenode.rpc-address.ns1", "localhost:8020");
        conf.set("dfs.namenode.rpc-address.ns2", "localhost:8023");
        conf.set("dfs.namenode.service.rpc-address.ns1", "localhost:8024");
        conf.set("dfs.namenode.service.rpc-address.ns2", "localhost:8025");

        // Test code: call getNNServiceRpcAddressesForCluster()
        Map<String, Map<String, InetSocketAddress>> rpcAddresses = DFSUtil.getNNServiceRpcAddressesForCluster(conf);

        // Assertions: ensure the function returns the expected map
        assertNotNull("Service RPC addresses map should not be null", rpcAddresses);
        assertTrue("Service RPC addresses should contain 'ns1'", rpcAddresses.containsKey("ns1"));
        assertTrue("Service RPC addresses should contain 'ns2'", rpcAddresses.containsKey("ns2"));

        Map<String, InetSocketAddress> ns1Addresses = rpcAddresses.get("ns1");
        Map<String, InetSocketAddress> ns2Addresses = rpcAddresses.get("ns2");

        assertNotNull("Service RPC addresses for 'ns1' should not be null", ns1Addresses);
        assertNotNull("Service RPC addresses for 'ns2' should not be null", ns2Addresses);

        // Print values for debugging (optional, but shouldn't replace assertions)
        System.out.println("ns1 Service RPC Address: " + ns1Addresses);
        System.out.println("ns2 Service RPC Address: " + ns2Addresses);
    }

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
        assertTrue("Lifeline RPC addresses should contain 'ns1'", lifelineRpcAddresses.containsKey("ns1"));
        assertTrue("Lifeline RPC addresses should contain 'ns2'", lifelineRpcAddresses.containsKey("ns2"));

        Map<String, InetSocketAddress> ns1Addresses = lifelineRpcAddresses.get("ns1");
        Map<String, InetSocketAddress> ns2Addresses = lifelineRpcAddresses.get("ns2");

        assertNotNull("Lifeline RPC addresses for 'ns1' should not be null", ns1Addresses);
        assertNotNull("Lifeline RPC addresses for 'ns2' should not be null", ns2Addresses);

        // Print values for debugging (optional, but shouldn't replace assertions)
        System.out.println("ns1 Lifeline RPC Address: " + ns1Addresses);
        System.out.println("ns2 Lifeline RPC Address: " + ns2Addresses);
    }
}