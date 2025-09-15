package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.net.NetUtils;
import org.junit.Test;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.net.URI;
import com.google.common.collect.Sets;

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
        // 1. Use the API correctly: Create and configure a `Configuration` object.
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions.
        // Set `dfs.internal.nameservices` to an empty value.
        conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "");
        Collection<String> internalNameServices = DFSUtil.getInternalNameServices(conf);
        assertTrue("Internal nameservices should be empty for this test", internalNameServices.isEmpty());

        // Define fallback nameservices under `dfs.nameservices`.
        String nameservice1 = "nameservice1";
        String nameservice2 = "nameservice2";
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, nameservice1 + "," + nameservice2);

        // Configure RPC addresses for fallback nameservices.
        conf.set(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY + "." + nameservice1, "localhost:8020");
        conf.set(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY + "." + nameservice2, "localhost:8021");

        // Ensure the configuration is properly set up.
        Collection<String> fallbackNameServices = conf.getTrimmedStringCollection(DFSConfigKeys.DFS_NAMESERVICES);
        assertFalse("Fallback nameservices should exist in the configuration", fallbackNameServices.isEmpty());

        // 3. Test code: Call the function to validate its behavior under test conditions.
        Map<String, Map<String, InetSocketAddress>> rpcAddresses = DFSUtil.getNNServiceRpcAddressesForCluster(conf);

        // 4. Code after testing: Verify the results.
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
        // 1. Use the API correctly: Create and configure a `Configuration` object.
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions.
        // Set `dfs.internal.nameservices` to an empty value.
        conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "");
        Collection<String> internalNameServices = DFSUtil.getInternalNameServices(conf);
        assertTrue("Internal nameservices should be empty for this test", internalNameServices.isEmpty());

        // Define fallback nameservices under `dfs.nameservices`.
        String nameservice1 = "nameservice1";
        String nameservice2 = "nameservice2";
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, nameservice1 + "," + nameservice2);

        // Configure Lifeline RPC addresses for fallback nameservices.
        conf.set(DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY + "." + nameservice1, "localhost:8120");
        conf.set(DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY + "." + nameservice2, "localhost:8121");

        // Ensure the configuration is properly set up.
        Collection<String> fallbackNameServices = conf.getTrimmedStringCollection(DFSConfigKeys.DFS_NAMESERVICES);
        assertFalse("Fallback nameservices should exist in the configuration", fallbackNameServices.isEmpty());

        // 3. Test code: Call the function to validate its behavior under test conditions.
        Map<String, Map<String, InetSocketAddress>> lifelineRpcAddresses = DFSUtil.getNNLifelineRpcAddressesForCluster(conf);

        // 4. Code after testing: Verify the results.
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
        // 1. Use the API correctly: Create and configure a `Configuration` object.
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions.
        // Set `dfs.internal.nameservices` to an empty value.
        conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "");
        Collection<String> internalNameServices = DFSUtil.getInternalNameServices(conf);
        assertTrue("Internal nameservices should be empty for this test", internalNameServices.isEmpty());

        // Define fallback nameservices under `dfs.nameservices`.
        String nameservice1 = "nameservice1";
        String nameservice2 = "nameservice2";
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, nameservice1 + "," + nameservice2);

        // Configure URIs for fallback nameservices.
        conf.set(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY + "." + nameservice1, "localhost:8020");
        conf.set(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY + "." + nameservice2, "localhost:8021");

        // Ensure the configuration is properly set up.
        Collection<String> fallbackNameServices = conf.getTrimmedStringCollection(DFSConfigKeys.DFS_NAMESERVICES);
        assertFalse("Fallback nameservices should exist in the configuration", fallbackNameServices.isEmpty());

        // 3. Test code: Call the function to validate its behavior under test conditions.
        Collection<URI> rpcUris = DFSUtil.getInternalNsRpcUris(conf);

        // 4. Code after testing: Verify the results.
        assertFalse("RPC URIs should not be empty", rpcUris.isEmpty());
        assertTrue("RPC URIs should include the address of nameservice1",
                rpcUris.contains(URI.create("hdfs://localhost:8020")));
        assertTrue("RPC URIs should include the address of nameservice2",
                rpcUris.contains(URI.create("hdfs://localhost:8021")));
    }
}