package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;

public class TestDFSUtil {

    @Test
    // Test to verify that the function throws an IOException when dfs.internal.nameservices includes undefined nameservices.
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getNNServiceRpcAddressesForCluster_withInvalidInternalNameservices() {
        // Prepare the test conditions
        Configuration conf = new Configuration();

        // Configure valid nameservices
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2");

        // Add an invalid internal nameservice
        conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "undefined_ns");

        // Test code
        try {
            // Call the method under test
            DFSUtil.getNNServiceRpcAddressesForCluster(conf);
        } catch (IOException e) {
            // Assert that the exception mentions the undefined nameservice
            String expectedMessage = "Unknown nameservice: undefined_ns";
            assert e.getMessage().contains(expectedMessage) : "Exception message does not contain expected message.";
        }
    }

    @Test
    // Test to verify that the function throws an IOException when dfs.internal.nameservices includes undefined nameservices for lifeline RPC addresses.
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getNNLifelineRpcAddressesForCluster_withInvalidInternalNameservices() {
        // Prepare the test conditions
        Configuration conf = new Configuration();

        // Configure valid nameservices
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2");

        // Add an invalid internal nameservice
        conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "undefined_ns");

        // Also simulate the lifeline RPC address configuration if needed
        conf.set("dfs.namenode.lifeline.rpc-address.ns1", "localhost:9002");
        conf.set("dfs.namenode.lifeline.rpc-address.ns2", "localhost:9003");

        // Test code
        try {
            // Call the method under test
            DFSUtil.getNNLifelineRpcAddressesForCluster(conf);
        } catch (IOException e) {
            // Assert that the exception mentions the undefined nameservice
            String expectedMessage = "Unknown nameservice: undefined_ns";
            assert e.getMessage().contains(expectedMessage) : "Exception message does not contain expected message.";
        }
    }

    @Test
    // Test to verify that the internal nameservices are correctly fetched from the configuration.
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getInternalNameServices() {
        // Prepare the test conditions
        Configuration conf = new Configuration();

        // Add internal nameservices to the configuration
        conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "ns1,ns2");

        // Test code
        Collection<String> internalNameServices = DFSUtil.getInternalNameServices(conf);

        // Assert that the internal nameservices are correctly fetched
        assert internalNameServices.contains("ns1") : "Internal nameservices do not include ns1.";
        assert internalNameServices.contains("ns2") : "Internal nameservices do not include ns2.";
    }

    @Test
    // Test to verify that the URI for each internal nameservice is properly returned.
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getInternalNsRpcUris() {
        // Prepare the test conditions
        Configuration conf = new Configuration();

        // Add internal nameservices to the configuration
        conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "ns1,ns2");

        // Add the RPC addresses for the nameservices
        conf.set("dfs.namenode.rpc-address.ns1", "localhost:9000");
        conf.set("dfs.namenode.rpc-address.ns2", "localhost:9001");

        // Test code
        Collection<URI> internalUris = DFSUtil.getInternalNsRpcUris(conf);

        // Assert that URIs are returned correctly for the internal nameservices
        boolean containsNs1 = false;
        boolean containsNs2 = false;
        for (URI uri : internalUris) {
            if (uri.toString().equals("hdfs://localhost:9000")) {
                containsNs1 = true;
            }
            if (uri.toString().equals("hdfs://localhost:9001")) {
                containsNs2 = true;
            }
        }
        assert containsNs1 : "Internal URIs do not include ns1.";
        assert containsNs2 : "Internal URIs do not include ns2.";
    }
}