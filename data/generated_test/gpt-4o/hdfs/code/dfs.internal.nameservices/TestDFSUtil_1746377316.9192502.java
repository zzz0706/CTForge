package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;

public class TestDFSUtil {

    @Test
    // Test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getNNLifelineRpcAddressesForCluster_with_empty_internal_nameservices() throws Exception {
        // Step 1: Prepare the Configuration object
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2"); // Define valid nameservices
        conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, ""); // Set internal nameservices to empty
        conf.set(DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY + ".ns1", "localhost:8020"); // Configure lifeline RPC address for ns1
        conf.set(DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY + ".ns2", "localhost:8021"); // Configure lifeline RPC address for ns2

        // Step 2: Retrieve nameservices from the configuration
        Collection<String> nameservices = conf.getTrimmedStringCollection(DFSConfigKeys.DFS_NAMESERVICES);

        // Step 3: Mock valid lifeline RPC addresses mapped to nameservices using DFSUtilClient API
        Map<String, Map<String, InetSocketAddress>> mockedLifelineAddresses = DFSUtilClient.getAddressesForNsIds(
            conf, nameservices, null, DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY
        );

        // Step 4: Execute the method being tested
        Map<String, Map<String, InetSocketAddress>> actualLifelineAddresses = DFSUtil.getNNLifelineRpcAddressesForCluster(conf);

        // Step 5: Assert that the returned lifeline RPC addresses match those of the fallback nameservices
        assertEquals("Returned lifeline RPC addresses do not match the mocked addresses.", mockedLifelineAddresses, actualLifelineAddresses);
    }

    @Test
    // Test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getNNServiceRpcAddressesForCluster_with_internal_nameservices() throws Exception {
        // Step 1: Prepare the Configuration object
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2"); // Define valid nameservices
        conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "ns1"); // Set internal nameservices explicitly
        conf.set(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY + ".ns1", "localhost:8020"); // Configure service RPC address for ns1
        conf.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + ".ns1", "localhost:8030"); // Configure RPC address for ns1

        // Step 2: Retrieve internal nameservices from the configuration
        Collection<String> internalNameServices = conf.getTrimmedStringCollection(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY);

        // Step 3: Mock valid service RPC addresses mapped to internal nameservices using DFSUtilClient API
        Map<String, Map<String, InetSocketAddress>> mockedServiceAddresses = DFSUtilClient.getAddressesForNsIds(
            conf, internalNameServices, null, DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY
        );

        // Step 4: Execute the method being tested
        Map<String, Map<String, InetSocketAddress>> actualServiceAddresses = DFSUtil.getNNServiceRpcAddressesForCluster(conf);

        // Step 5: Assert that the returned service RPC addresses match those of the internal nameservices
        assertEquals("Returned service RPC addresses do not match the mocked addresses.", mockedServiceAddresses, actualServiceAddresses);
    }

    @Test
    // Test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getInternalNsRpcUris() throws Exception {
        // Step 1: Prepare the Configuration object
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2"); // Define valid nameservices
        conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "ns2"); // Set internal nameservices explicitly
        conf.set(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY + ".ns2", "localhost:8021"); // Configure service RPC address for ns2
        conf.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + ".ns2", "localhost:8031"); // Configure RPC address for ns2

        // Step 2: Retrieve internal nameservices URIs using DFSUtil API
        Collection<String> internalNameServices = DFSUtil.getInternalNameServices(conf);

        // Mock URIs for internal nameservices
        Collection<java.net.URI> mockedRpcUris = DFSUtil.getNameServiceUris(
            conf, internalNameServices, DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY
        );

        // Step 3: Execute the method being tested
        Collection<java.net.URI> actualRpcUris = DFSUtil.getInternalNsRpcUris(conf);

        // Step 4: Assert that the returned URIs match the mocked internal nameservice URIs
        assertEquals("Returned internal nameservice URIs do not match the mocked URIs.", mockedRpcUris, actualRpcUris);
    }
}