package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.net.NetUtils;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;

public class TestDFSUtil {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testGetNNServiceRpcAddressesForCluster_withInvalidInternalNameservices() throws IOException {
        // 1. Prepare the test conditions
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "undefined_ns"); // Undefined nameservice
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2"); // Valid nameservices

        try {
            // 2. Test code
            // Attempt to fetch NameNode Service RPC addresses
            Map<String, Map<String, InetSocketAddress>> addresses = DFSUtil.getNNServiceRpcAddressesForCluster(conf);

            // If no exception was thrown, fail the test
            assert false : "Expected IOException was not thrown.";
        } catch (IOException e) {
            // Validate the exception
            String exceptionMessage = e.getMessage();
            assert exceptionMessage.contains("Unknown nameservice: undefined_ns") :
                "Expected exception containing 'Unknown nameservice: undefined_ns', but got: " + exceptionMessage;
        }
    }

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testGetNNLifelineRpcAddressesForCluster_withInvalidInternalNameservices() throws IOException {
        // 1. Prepare the test conditions
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "undefined_ns"); // Undefined nameservice
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2"); // Valid nameservices

        try {
            // 2. Test code
            // Attempt to fetch NameNode Lifeline RPC addresses
            Map<String, Map<String, InetSocketAddress>> lifelineAddresses = DFSUtil.getNNLifelineRpcAddressesForCluster(conf);

            // If no exception was thrown, fail the test
            assert false : "Expected IOException was not thrown.";
        } catch (IOException e) {
            // Validate the exception
            String exceptionMessage = e.getMessage();
            assert exceptionMessage.contains("Unknown nameservice: undefined_ns") :
                "Expected exception containing 'Unknown nameservice: undefined_ns', but got: " + exceptionMessage;
        }
    }

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testGetInternalNameServices_withUndefinedInternalNameservices() {
        // 1. Prepare the test conditions
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "undefined_ns"); // Undefined nameservice
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2"); // Valid nameservices

        // 2. Test code
        Collection<String> internalNameServices = DFSUtil.getInternalNameServices(conf);

        // Validate the internal nameservices
        assert internalNameServices.size() == 1 : "Expected internal nameservices size of 1.";
        assert internalNameServices.contains("undefined_ns") : "Expected 'undefined_ns' in internal nameservices.";
    }

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testGetInternalNsRpcUris_withEmptyConfiguration() {
        // 1. Prepare the test conditions
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "");

        // 2. Test code
        Collection<URI> rpcUris = DFSUtil.getInternalNsRpcUris(conf);

        // Validate the result
        assert rpcUris.isEmpty() : "Expected empty URI collection when dfs.internal.nameservices configuration is absent.";
    }
}