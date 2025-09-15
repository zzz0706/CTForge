package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestDFSUtil {

    @Test
    // Test code for `test_getNNServiceRpcAddressesForCluster_with_unknown_internal_nameservices`
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testGetNNServiceRpcAddressesForClusterWithUnknownInternalNameservices() {
        Configuration conf = new Configuration();

        // Prepare the test conditions
        // Fetch the current nameservices configuration via APIs
        Collection<String> existingNameServices = conf.getTrimmedStringCollection(DFSConfigKeys.DFS_NAMESERVICES);
        
        // Adding mock internal nameservice IDs that do not exist in `dfs.nameservices`
        String[] mockInternalNameServices = {"mock-ns-1", "mock-ns-2"};
        conf.setStrings(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, mockInternalNameServices);

        // Ensure `dfs.nameservices` does not match `dfs.internal.nameservices`
        if (existingNameServices.isEmpty()) {
            conf.setStrings(DFSConfigKeys.DFS_NAMESERVICES, "valid-ns-1", "valid-ns-2");
        } else {
            conf.setStrings(DFSConfigKeys.DFS_NAMESERVICES, existingNameServices.toArray(new String[0]));
        }

        // Test code: Call `getNNServiceRpcAddressesForCluster` and validate functionality
        try {
            DFSUtil.getNNServiceRpcAddressesForCluster(conf);
            fail("Expected IOException for unknown nameservices was not thrown.");
        } catch (IOException e) {
            // Assert: Validate the exception message for `Unknown nameservice`
            String expectedMessagePrefix = "Unknown nameservice: ";
            assertTrue("Exception message should contain the expected prefix",
                    e.getMessage().contains(expectedMessagePrefix));
        }
    }
}