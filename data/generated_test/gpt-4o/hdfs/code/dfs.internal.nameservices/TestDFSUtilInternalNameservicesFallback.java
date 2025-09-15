package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.net.NetUtils;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestDFSUtilInternalNameservicesFallback {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getNNServiceRpcAddressesForCluster_withEmptyInternalNameservices() throws Exception {
        // 1. Prepare the test conditions
        Configuration configuration = new Configuration();

        // Set valid nameservices and namenodes configurations
        configuration.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2");
        configuration.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + ".ns1", "nn1,nn2");
        configuration.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + ".ns1.nn1", "localhost:8020");
        configuration.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + ".ns1.nn2", "localhost:8021");
        configuration.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + ".ns2", "nn3,nn4");
        configuration.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + ".ns2.nn3", "localhost:8022");
        configuration.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + ".ns2.nn4", "localhost:8023");

        // Ensure dfs.internal.nameservices is empty
        assertTrue("DFS_INTERNAL_NAMESERVICES should be empty.",
                   configuration.getTrimmedStringCollection(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY).isEmpty());

        // 2. Test code
        Map<String, Map<String, InetSocketAddress>> rpcAddresses =
            DFSUtil.getNNServiceRpcAddressesForCluster(configuration);

        // 3. Verify the result
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
    public void test_getNNLifelineRpcAddressesForCluster_withEmptyInternalNameservices() throws Exception {
        // 1. Prepare the test conditions
        Configuration configuration = new Configuration();

        // Set valid nameservices and their lifeline RPC configurations
        configuration.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2");
        configuration.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + ".ns1", "nn1,nn2");
        configuration.set(DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY + ".ns1.nn1", "localhost:9000");
        configuration.set(DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY + ".ns1.nn2", "localhost:9001");
        configuration.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + ".ns2", "nn3,nn4");
        configuration.set(DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY + ".ns2.nn3", "localhost:9002");
        configuration.set(DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY + ".ns2.nn4", "localhost:9003");

        // Ensure dfs.internal.nameservices is empty
        assertTrue("DFS_INTERNAL_NAMESERVICES should be empty.",
                   configuration.getTrimmedStringCollection(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY).isEmpty());

        // 2. Test code
        Map<String, Map<String, InetSocketAddress>> lifelineAddresses =
            DFSUtil.getNNLifelineRpcAddressesForCluster(configuration);

        // 3. Verify the result
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
    public void test_getInternalNsRpcUris_withEmptyInternalNameservices() throws Exception {
        // 1. Prepare the test conditions
        Configuration configuration = new Configuration();

        // Set valid nameservices and RPC URIs
        configuration.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2");
        configuration.set(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY + ".ns1", "localhost:8025");
        configuration.set(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY + ".ns2", "localhost:8026");

        // Ensure dfs.internal.nameservices is empty
        assertTrue("DFS_INTERNAL_NAMESERVICES should be empty.",
                   configuration.getTrimmedStringCollection(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY).isEmpty());

        // 2. Test code
        Collection<URI> internalUris = DFSUtil.getInternalNsRpcUris(configuration);

        // 3. Verify the result
        assertTrue("The result should contain URIs for configured nameservices.", !internalUris.isEmpty());
        for (URI uri : internalUris) {
            assertNotNull("Each URI in the result should not be null.", uri);
        }
    }
}