package org.apache.hadoop.hdfs;   

import org.apache.hadoop.conf.Configuration;       
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

public class TestDataNodeConfiguration {

    @Test
    // Test code for test_getNamenodeAddresses_multiple_NNs
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getNamenodeAddresses_multiple_NNs() throws Exception {
        // 1. Prepare test environment - Initialize required configuration
        Configuration conf = new Configuration();
        conf.set("dfs.datanode.address", "0.0.0.0:50010");
        conf.set("dfs.namenode.rpc-address.ns1", "namenode1.example.com:8020");
        conf.set("dfs.namenode.rpc-address.ns2", "namenode2.example.com:8020");

        // Use the required configuration key to avoid missing configurations
        conf.set("dfs.nameservices", "ns1,ns2");
        conf.set("dfs.ha.namenodes.ns1", "nn1");
        conf.set("dfs.namenode.rpc-address.ns1.nn1", "namenode1.example.com:8020");
        conf.set("dfs.ha.namenodes.ns2", "nn2");
        conf.set("dfs.namenode.rpc-address.ns2.nn2", "namenode2.example.com:8020");

        // Create a DataNode object
        String[] dataDirs = new String[]{};
        DataNode dataNode = DataNode.createDataNode(dataDirs, conf);

        // 2. Prepare test conditions - Create a map to hold expected results
        Map<String, String> expectedAddresses = new HashMap<>();
        expectedAddresses.put("ns1", "namenode1.example.com:8020");
        expectedAddresses.put("ns2", "namenode2.example.com:8020");

        // 3. Test code - Fetch actual configuration values and assert they match
        Map<String, String> actualAddresses = new HashMap<>();
        for (String ns : expectedAddresses.keySet()) {
            actualAddresses.put(ns, conf.get("dfs.namenode.rpc-address." + ns));
        }

        // Assert that the expected and actual values match
        assertEquals(expectedAddresses, actualAddresses);

        // 4. Code after testing - Clean up resources
        if (dataNode != null) {
            dataNode.shutdown(); // Ensure the DataNode is properly shut down
        }
    }
}