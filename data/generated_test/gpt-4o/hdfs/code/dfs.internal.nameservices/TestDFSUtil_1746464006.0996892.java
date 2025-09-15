package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.junit.Test;
import java.io.IOException;

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
        conf.set("dfs.nameservices", "ns1,ns2");
        
        // Add an invalid internal nameservice
        conf.set("dfs.internal.nameservices", "undefined_ns");

        // Test code
        try {
            // Call the method under test
            DFSUtil.getNNServiceRpcAddressesForCluster(conf);
        } catch (IOException e) {
            // Assert that the exception mentions the undefined nameservice
            assert e.getMessage().contains("Unknown nameservice: undefined_ns");
        }
    }
}