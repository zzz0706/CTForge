package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.net.NetUtils;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestDFSUtil {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testGetNNServiceRpcAddressesForCluster_with_valid_internal_nameservices() throws Exception {
        // Step 1: Initialize configuration
        Configuration conf = new Configuration();

        // Set required configurations using the hdfs 2.8.5 API
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1");
        conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "ns1");
        conf.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + ".ns1", "nn1");
        conf.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + ".ns1.nn1", "namenode1.example.com:8020");

        // Step 2: Prepare the test conditions
        Collection<String> validInternalNameServices = Collections.singletonList("ns1");
        Map<String, Map<String, InetSocketAddress>> expectedRpcAddresses = new HashMap<>();
        Map<String, InetSocketAddress> rpcAddressMap = new HashMap<>();

        // Use NetUtils.createSocketAddr to properly create InetSocketAddress
        rpcAddressMap.put("nn1", NetUtils.createSocketAddr("namenode1.example.com:8020"));
        expectedRpcAddresses.put("ns1", rpcAddressMap);

        // Step 3: Execute the method under test
        Map<String, Map<String, InetSocketAddress>> returnedRpcAddresses =
                DFSUtil.getNNServiceRpcAddressesForCluster(conf);

        // Step 4: Verify the results
        assertNotNull(returnedRpcAddresses);
        assertEquals(expectedRpcAddresses, returnedRpcAddresses);
    }

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testGetNNLifelineRpcAddressesForCluster_with_valid_internal_nameservices() throws Exception {
        // Step 1: Initialize configuration
        Configuration conf = new Configuration();

        // Set required configurations using the hdfs 2.8.5 API
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1");
        conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "ns1");
        conf.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + ".ns1", "nn1");
        conf.set(DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY + ".ns1.nn1", "namenode1.example.com:8020");

        // Step 2: Prepare the test conditions
        Collection<String> validInternalNameServices = Collections.singletonList("ns1");
        Map<String, Map<String, InetSocketAddress>> expectedRpcAddresses = new HashMap<>();
        Map<String, InetSocketAddress> lifelineRpcAddressMap = new HashMap<>();

        // Use NetUtils.createSocketAddr to properly create InetSocketAddress
        lifelineRpcAddressMap.put("nn1", NetUtils.createSocketAddr("namenode1.example.com:8020"));
        expectedRpcAddresses.put("ns1", lifelineRpcAddressMap);

        // Step 3: Execute the method under test
        Map<String, Map<String, InetSocketAddress>> returnedRpcAddresses =
                DFSUtil.getNNLifelineRpcAddressesForCluster(conf);

        // Step 4: Verify the results
        assertNotNull(returnedRpcAddresses);
        assertEquals(expectedRpcAddresses, returnedRpcAddresses);
    }
}