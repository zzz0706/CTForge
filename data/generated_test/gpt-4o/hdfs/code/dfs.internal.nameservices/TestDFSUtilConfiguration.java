package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.junit.Test;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.net.URI;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class TestDFSUtilConfiguration {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getNNServiceRpcAddressesForCluster_with_empty_internal_nameservices() throws IOException {
        // 1. Use the API correctly: Create a configuration object and use DFSConfigKeys to define keys.
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions.
        // Set up the internal nameservices key to be empty.
        conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "");
        Collection<String> internalNameServices = conf.getTrimmedStringCollection(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY);
        assertTrue("Internal nameservices should be empty for this test", internalNameServices.isEmpty());

        // Set fallback nameservices under `dfs.nameservices`.
        String nameservice1 = "nameservice1";
        String nameservice2 = "nameservice2";
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, nameservice1 + "," + nameservice2);

        // Ensure fallback configuration exists.
        Collection<String> fallbackNameServices = conf.getTrimmedStringCollection(DFSConfigKeys.DFS_NAMESERVICES);
        assertFalse("Fallback nameservices should exist in the configuration", fallbackNameServices.isEmpty());

        // Configure RPC addresses for fallback nameservices.
        conf.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + nameservice1, "localhost:8020");
        conf.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + nameservice2, "localhost:8021");

        // 3. Test code: Call the function to verify behavior with fallback configuration.
        Map<String, Map<String, InetSocketAddress>> rpcAddresses = DFSUtil.getNNServiceRpcAddressesForCluster(conf);

        // 4. Code after testing: Assert that valid RPC addresses are returned for fallback nameservices.
        assertFalse("RPC addresses should not be empty", rpcAddresses.isEmpty());
        assertTrue("RPC addresses should contain nameservice1", rpcAddresses.containsKey(nameservice1));
        assertTrue("RPC addresses should contain nameservice2", rpcAddresses.containsKey(nameservice2));
    }

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getNNLifelineRpcAddressesForCluster_with_empty_internal_nameservices() throws IOException {
        // 1. Use the API correctly: Create a configuration object and use DFSConfigKeys to define keys.
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions.
        // Set up the internal nameservices key to be empty.
        conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "");
        Collection<String> internalNameServices = conf.getTrimmedStringCollection(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY);
        assertTrue("Internal nameservices should be empty for this test", internalNameServices.isEmpty());

        // Set fallback nameservices under `dfs.nameservices`.
        String nameservice1 = "nameservice1";
        String nameservice2 = "nameservice2";
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, nameservice1 + "," + nameservice2);

        // Ensure fallback configuration exists.
        Collection<String> fallbackNameServices = conf.getTrimmedStringCollection(DFSConfigKeys.DFS_NAMESERVICES);
        assertFalse("Fallback nameservices should exist in the configuration", fallbackNameServices.isEmpty());

        // Configure Lifeline RPC addresses for fallback nameservices.
        conf.set(DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY + "." + nameservice1, "localhost:8120");
        conf.set(DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY + "." + nameservice2, "localhost:8121");

        // 3. Test code: Call the function to verify behavior with fallback configuration.
        Map<String, Map<String, InetSocketAddress>> lifelineRpcAddresses = DFSUtil.getNNLifelineRpcAddressesForCluster(conf);

        // 4. Code after testing: Assert that valid Lifeline RPC addresses are returned for fallback nameservices.
        assertFalse("Lifeline RPC addresses should not be empty", lifelineRpcAddresses.isEmpty());
        assertTrue("Lifeline RPC addresses should contain nameservice1", lifelineRpcAddresses.containsKey(nameservice1));
        assertTrue("Lifeline RPC addresses should contain nameservice2", lifelineRpcAddresses.containsKey(nameservice2));
    }

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getInternalNsRpcUris_with_empty_internal_nameservices() {
        // 1. Use the API correctly: Create a configuration object and use DFSConfigKeys to define keys.
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions.
        // Set up the internal nameservices key to be empty.
        conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "");
        Collection<String> internalNameServices = DFSUtil.getInternalNameServices(conf);
        assertTrue("Internal nameservices should be empty for this test", internalNameServices.isEmpty());

        // Set fallback nameservices under `dfs.nameservices`.
        String nameservice1 = "nameservice1";
        String nameservice2 = "nameservice2";
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, nameservice1 + "," + nameservice2);

        // Configure URIs for fallback nameservices.
        conf.set(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY + "." + nameservice1, "localhost:8020");
        conf.set(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY + "." + nameservice2, "localhost:8021");

        // 3. Test code: Call `getInternalNsRpcUris` to verify behavior with fallback configuration.
        Collection<URI> rpcUris = DFSUtil.getInternalNsRpcUris(conf);

        // 4. Code after testing: Assert that valid URIs are returned for fallback nameservices.
        assertFalse("RPC URIs should not be empty", rpcUris.isEmpty());
        assertTrue("RPC URIs should include the address of nameservice1",
                rpcUris.contains(URI.create("hdfs://localhost:8020")));
        assertTrue("RPC URIs should include the address of nameservice2",
                rpcUris.contains(URI.create("hdfs://localhost:8021")));
    }
}