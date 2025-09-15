package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

public class TestDFSUtilConfigurations {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getNNServiceRpcAddressesForCluster_withEmptyInternalNameservices() throws Exception {
        // 1. Prepare the test conditions
        Configuration configuration = new Configuration();

        // Simulate configurations for nameservices and their respective namenodes
        configuration.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2");
        configuration.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + ".ns1", "nn1,nn2");
        configuration.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + ".ns1.nn1", "localhost:8020");
        configuration.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + ".ns1.nn2", "localhost:8021");
        configuration.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + ".ns2", "nn3,nn4");
        configuration.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + ".ns2.nn3", "localhost:8022");
        configuration.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + ".ns2.nn4", "localhost:8023");

        // Ensure dfs.internal.nameservices is empty to trigger fallback to dfs.nameservices
        assertTrue("DFS_INTERNAL_NAMESERVICES should be empty.", 
                   configuration.getTrimmedStringCollection(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY).isEmpty());

        // 2. Test code: get NN Service RPC addresses for the cluster
        Map<String, Map<String, InetSocketAddress>> rpcAddresses = 
                DFSUtil.getNNServiceRpcAddressesForCluster(configuration);

        // 3. Verify the result by checking correctness of the returned structure
        Collection<String> configuredNameservices = configuration.getTrimmedStringCollection(DFSConfigKeys.DFS_NAMESERVICES);
        for (String nameservice : configuredNameservices) {
            assertTrue("The result should contain the nameservice " + nameservice, 
                       rpcAddresses.containsKey(nameservice));
            Map<String, InetSocketAddress> nnMap = rpcAddresses.get(nameservice);
            assertNotNull("The namenode map for " + nameservice + " should not be null.", nnMap);

            Collection<String> haNamenodes = configuration.getTrimmedStringCollection(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + "." + nameservice);
            for (String nn : haNamenodes) {
                assertTrue("The result should contain the namenode " + nn + " for nameservice " + nameservice, 
                           nnMap.containsKey(nn));
            }
        }
    }

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getNNLifelineRpcAddressesForCluster_withFallbackToNameservices() throws Exception {
        // 1. Prepare the test conditions
        Configuration configuration = new Configuration();

        // Simulate configurations for nameservices and their lifeline RPC addresses
        configuration.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2");
        configuration.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + ".ns1", "nn1,nn2");
        configuration.set(DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY + ".ns1.nn1", "localhost:9000");
        configuration.set(DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY + ".ns1.nn2", "localhost:9001");
        configuration.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + ".ns2", "nn3,nn4");
        configuration.set(DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY + ".ns2.nn3", "localhost:9002");
        configuration.set(DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY + ".ns2.nn4", "localhost:9003");

        // Ensure dfs.internal.nameservices is empty to trigger fallback to dfs.nameservices
        assertTrue("DFS_INTERNAL_NAMESERVICES should be empty.", 
                   configuration.getTrimmedStringCollection(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY).isEmpty());

        // 2. Test code: get NN Lifeline RPC addresses for the cluster
        Map<String, Map<String, InetSocketAddress>> lifelineAddresses = 
                DFSUtil.getNNLifelineRpcAddressesForCluster(configuration);

        // 3. Verify the result by checking correctness of the returned structure
        Collection<String> configuredNameservices = configuration.getTrimmedStringCollection(DFSConfigKeys.DFS_NAMESERVICES);
        for (String nameservice : configuredNameservices) {
            assertTrue("The result should contain the nameservice " + nameservice, 
                       lifelineAddresses.containsKey(nameservice));
            Map<String, InetSocketAddress> lifelineMap = lifelineAddresses.get(nameservice);
            assertNotNull("The lifeline map for " + nameservice + " should not be null.", lifelineMap);

            Collection<String> haNamenodes = configuration.getTrimmedStringCollection(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + "." + nameservice);
            for (String nn : haNamenodes) {
                assertTrue("The result should contain the lifeline RPC address for " + nn + " in nameservice " + nameservice, 
                           lifelineMap.containsKey(nn));
            }
        }
    }

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getInternalNsRpcUris_withFallbackToNameservices() throws Exception {
        // 1. Prepare the test conditions
        Configuration configuration = new Configuration();

        // Simulate configurations for nameservices and their respective RPC addresses
        configuration.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2");
        configuration.set(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY + ".ns1", "localhost:8025");
        configuration.set(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY + ".ns2", "localhost:8026");

        // Ensure dfs.internal.nameservices is empty to trigger fallback
        assertTrue("DFS_INTERNAL_NAMESERVICES should be empty.", 
                   configuration.getTrimmedStringCollection(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY).isEmpty());

        // 2. Test code: get internal namespace RPC URIs
        Collection<URI> internalNsUris = DFSUtil.getInternalNsRpcUris(configuration);

        // 3. Verify the returned URIs
        assertTrue("The result should contain URIs for configured nameservices.", !internalNsUris.isEmpty());
        for (URI uri : internalNsUris) {
            assertNotNull("URI should not be null.", uri);
        }
    }
}