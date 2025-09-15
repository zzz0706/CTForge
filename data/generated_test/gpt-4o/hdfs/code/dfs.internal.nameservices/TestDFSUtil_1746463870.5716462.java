package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

public class TestDFSUtil {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getNNServiceRpcAddressesForCluster_withEmptyInternalNameservices() throws Exception {
        // 1. Prepare the test conditions
        Configuration configuration = new Configuration();

        // Simulate test-specific configurations
        configuration.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2");
        configuration.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + ".ns1", "nn1,nn2");
        configuration.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + ".ns1.nn1", "localhost:8020");
        configuration.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + ".ns1.nn2", "localhost:8021");
        configuration.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + ".ns2", "nn3,nn4");
        configuration.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + ".ns2.nn3", "localhost:8022");
        configuration.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + ".ns2.nn4", "localhost:8023");

        // 2. Retrieve nameservices using the API
        Collection<String> allNameservices = configuration.getTrimmedStringCollection(DFSConfigKeys.DFS_NAMESERVICES);
        assertTrue("DFS_NAMESERVICES should not be empty for this test.", !allNameservices.isEmpty());

        // Ensure dfs.internal.nameservices is empty
        Collection<String> internalNameservices = configuration.getTrimmedStringCollection(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY);
        assertTrue("DFS_INTERNAL_NAMESERVICES should start empty for the fallback test.", internalNameservices.isEmpty());

        // 3. Test code
        Map<String, Map<String, InetSocketAddress>> namenodeAddresses = DFSUtil.getNNServiceRpcAddressesForCluster(configuration);

        // 4. Verify that the returned addresses correctly use the fallback from dfs.nameservices
        for (String nameservice : allNameservices) {
            assertTrue("Namenode addresses should include entries for " + nameservice,
                    namenodeAddresses.containsKey(nameservice));

            // Ensure the maps for the nameservice include all relevant namenodes
            Map<String, InetSocketAddress> nnMap = namenodeAddresses.get(nameservice);
            assertNotNull("Namenode map for " + nameservice + " should not be null.", nnMap);

            Collection<String> haNamenodes = configuration.getTrimmedStringCollection(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + "." + nameservice);
            for (String nn : haNamenodes) {
                assertTrue("Namenode address map for " + nameservice + " should include " + nn,
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

        // Simulate test-specific configurations
        configuration.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2");
        configuration.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + ".ns1", "nn1,nn2");
        configuration.set(DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY + ".ns1.nn1", "localhost:9000");
        configuration.set(DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY + ".ns1.nn2", "localhost:9001");
        configuration.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + ".ns2", "nn3,nn4");
        configuration.set(DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY + ".ns2.nn3", "localhost:9002");
        configuration.set(DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY + ".ns2.nn4", "localhost:9003");

        // Ensure dfs.internal.nameservices is empty
        Collection<String> internalNameservices = configuration.getTrimmedStringCollection(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY);
        assertTrue("DFS_INTERNAL_NAMESERVICES should start empty for the fallback test.", internalNameservices.isEmpty());

        // 2. Retrieve nameservices using the API
        Collection<String> allNameservices = configuration.getTrimmedStringCollection(DFSConfigKeys.DFS_NAMESERVICES);
        assertTrue("DFS_NAMESERVICES should not be empty for this test.", !allNameservices.isEmpty());

        // 3. Test code
        Map<String, Map<String, InetSocketAddress>> lifelineAddresses = DFSUtil.getNNLifelineRpcAddressesForCluster(configuration);

        // 4. Verify that the returned addresses match expectations
        for (String nameservice : allNameservices) {
            assertTrue("Lifeline addresses should include entries for " + nameservice,
                    lifelineAddresses.containsKey(nameservice));

            Map<String, InetSocketAddress> nnLifelineMap = lifelineAddresses.get(nameservice);
            assertNotNull("Lifeline map for " + nameservice + " should not be null.", nnLifelineMap);

            Collection<String> haNamenodes = configuration.getTrimmedStringCollection(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + "." + nameservice);
            for (String nn : haNamenodes) {
                assertTrue("Lifeline address for " + nameservice + " should include " + nn,
                        nnLifelineMap.containsKey(nn));
            }
        }
    }
}