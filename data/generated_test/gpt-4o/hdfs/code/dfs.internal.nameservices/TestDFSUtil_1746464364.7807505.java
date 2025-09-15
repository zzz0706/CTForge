package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.junit.Test;
import java.io.IOException;

public class TestDFSUtil {

    @Test
    // Test code to fix compilation issues
    // 1. Ensure correct usage of HDFS 2.8.5 API to obtain configuration values instead of hardcoding them.
    // 2. Prepare the test conditions.
    // 3. Execute the test code.
    // 4. Handle and validate the test output.
    public void testGetNNLifelineRpcAddressesForClusterWithInvalidInternalNameservices() {
        // 1. Prepare the test conditions
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "undefined_ns"); // Undefined nameservice
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2"); // Valid nameservices

        // 2. Test code
        try {
            // Call the target method which is expected to throw an exception
            DFSUtil.getNNLifelineRpcAddressesForCluster(conf);
        } catch (IOException e) {
            // 3. Validate the exception
            String exceptionMessage = e.getMessage();
            assert exceptionMessage.contains("Unknown nameservice: undefined_ns") : 
                "Expected exception containing 'Unknown nameservice: undefined_ns', but got: " + exceptionMessage;
            return; 
        }

        // 4. Fail the test if no exception was thrown
        assert false : "Expected IOException was not thrown.";
    }
}