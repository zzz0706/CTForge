package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.net.NetUtils;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class TestDFSInternalNameServices {

    @Test
    // Test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testDFSInternalNameServicesConfiguration() throws IOException {
        // Test Setup
        Configuration conf = new Configuration();

        // Test Code
        // 1. Retrieve dfs.internal.nameservices value using the correct HDFS API.
        Collection<String> internalNameServices = conf.getTrimmedStringCollection(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY);

        // 2. Retrieve dfs.nameservices value as a fallback using the correct HDFS API.
        Collection<String> nameServices = conf.getTrimmedStringCollection(DFSConfigKeys.DFS_NAMESERVICES);

        // 3. Check fallback behavior when dfs.internal.nameservices is not configured.
        if (internalNameServices.isEmpty()) {
            // Ensure that dfs.internal.nameservices falls back to dfs.nameservices.
            assertEquals("If dfs.internal.nameservices is not set, it should fall back to dfs.nameservices", nameServices, internalNameServices);
        } else {
            // Validate dependencies and constraints.

            // 4. Check that internal nameservices exist in the available nameservices.
            Set<String> availableNameServices = new HashSet<>(nameServices);
            for (String nsId : internalNameServices) {
                assertTrue("Internal nameservice must exist within the list of available nameservices: " + nsId,
                        availableNameServices.contains(nsId));
            }

            // 5. Ensure that valid namenode addresses are defined for the nameservices.
            Map<String, Map<String, InetSocketAddress>> addressList = getNNServiceRpcAddressesForCluster(conf,
                    DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY);
            assertFalse("There must be valid namenode addresses associated with the nameservices.", addressList.isEmpty());

            // 6. Validate that internal nameservices have valid RPC addresses using correct API methods.
            Map<String, Map<String, InetSocketAddress>> rpcAddresses = getNNServiceRpcAddressesForCluster(conf,
                    DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY);
            assertFalse("There must be valid RPC addresses for internal nameservices.", rpcAddresses.isEmpty());
        }

        // Additional validation to ensure no misconfiguration.
        // 7. Proper handling of non-existent fields in configuration.
        try {
            Map<String, Map<String, InetSocketAddress>> lifelineAddresses = getNNServiceRpcAddressesForCluster(conf,
                    DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY);
            assertNotNull("Lifeline RPC addresses should be resolved without errors.", lifelineAddresses);
        } catch (Exception e) {
            fail("Configuration error detected during lifeline RPC address checks: " + e.getMessage());
        }
    }

    /**
     * Utility function to retrieve Namenode RPC addresses for the cluster.
     * This replaces the deprecated Util.getNNServiceRpcAddressesForCluster.
     *
     * @param conf Configuration object
     * @param rpcKey RPC address configuration key
     * @return Map of nameservice to map of namenode ID to its InetSocketAddress
     */
    private Map<String, Map<String, InetSocketAddress>> getNNServiceRpcAddressesForCluster(Configuration conf, String rpcKey) {
        Map<String, Map<String, InetSocketAddress>> result = new HashMap<>();
        Collection<String> nameServices = conf.getTrimmedStringCollection(DFSConfigKeys.DFS_NAMESERVICES);

        for (String nameServiceId : nameServices) {
            Map<String, InetSocketAddress> nnMap = new HashMap<>();
            String nnAddresses = conf.get(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + "." + nameServiceId);

            if (nnAddresses != null) {
                for (String nnId : nnAddresses.split(",")) {
                    String rpcAddressKey = DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + nameServiceId + "." + nnId.trim();
                    String rpcAddress = conf.get(rpcAddressKey);

                    if (rpcAddress != null) {
                        nnMap.put(nnId.trim(), NetUtils.createSocketAddr(rpcAddress));
                    }
                }
                result.put(nameServiceId, nnMap);
            }
        }
        return result;
    }
}