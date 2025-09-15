package org.apache.hadoop.hdfs;   

import org.apache.hadoop.conf.Configuration;       
import org.apache.hadoop.hdfs.DFSConfigKeys;       
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.DFSUtil;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;

public class TestDFSUtil {   

    @Test 
    // Test code
    // 1. Use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions by mocking valid nameservices defined in `dfs.nameservices`.
    // 3. Execute the `getNNLifelineRpcAddressesForCluster` method using the prepared configuration.
    // 4. Assert that the returned addresses match the fallback nameservices' lifeline RPC addresses.
    public void test_getNNLifelineRpcAddressesForCluster_with_empty_internal_nameservices() throws Exception {
        // Step 1: Prepare the Configuration object
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2");

        // Step 2: Retrieve nameservices from the configuration
        Collection<String> nameservices = conf.getTrimmedStringCollection(DFSConfigKeys.DFS_NAMESERVICES);

        // Step 3: Mock addresses for the nameservices using DFSUtilClient API
        Map<String, Map<String, InetSocketAddress>> mockedAddresses = DFSUtilClient.getAddressesForNsIds(
            conf, nameservices, null, DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY
        );

        // Step 4: Execute the method being tested
        Map<String, Map<String, InetSocketAddress>> lifelineRpcAddresses = DFSUtil.getNNLifelineRpcAddressesForCluster(conf);

        // Step 5: Assert the results
        assertEquals("Expected lifeline RPC addresses do not match mocked addresses.", mockedAddresses, lifelineRpcAddresses);
    }
}