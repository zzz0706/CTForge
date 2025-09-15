package org.apache.hadoop.hdfs.protocol.datatransfer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.Test;

public class ReplaceDatanodeOnFailureTest {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_write_with_policy_NEVER() {
        // Prepare the test conditions: Create a Configuration object.
        Configuration conf = new Configuration();

        // Test code: Call ReplaceDatanodeOnFailure.write with Policy.NEVER and bestEffort set to false.
        ReplaceDatanodeOnFailure.write(ReplaceDatanodeOnFailure.Policy.NEVER, false, conf);

        // Retrieve the configuration values using the Configuration API.
        boolean enableKeyValue = conf.getBoolean(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY, 
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_DEFAULT
        );
        String policyKeyValue = conf.get(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY, 
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_DEFAULT
        );
        boolean bestEffortKeyValue = conf.getBoolean(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_KEY, 
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_DEFAULT
        );

        // Code after testing: Assert the expected results.
        assert enableKeyValue; // ENABLE_KEY should be set to true.
        assert "NEVER".equals(policyKeyValue); // POLICY_KEY should be set to 'NEVER'.
        assert !bestEffortKeyValue; // BEST_EFFORT_KEY should be set to false.
    }
}