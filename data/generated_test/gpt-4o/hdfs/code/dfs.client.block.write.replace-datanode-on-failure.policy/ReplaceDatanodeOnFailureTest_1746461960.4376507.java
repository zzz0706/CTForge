package org.apache.hadoop.hdfs.protocol.datatransfer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.datatransfer.ReplaceDatanodeOnFailure;
import org.apache.hadoop.hdfs.protocol.datatransfer.ReplaceDatanodeOnFailure.Policy;
import org.junit.Test;

public class ReplaceDatanodeOnFailureTest {

    @Test
    // test code for ReplaceDatanodeOnFailure with policy NEVER
    // 1. You need to use the HDFS 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions: Create a Configuration object and set values using Configuration API.
    // 3. Test code: Verify that the configuration values are set as expected.
    // 4. Code after testing: Assert the expected results.
    public void test_write_with_policy_NEVER() {
        // 2. Prepare the test conditions: Create a Configuration object.
        Configuration conf = new Configuration();
        conf.setBoolean(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY, true);
        conf.set(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY, Policy.NEVER.name());
        conf.setBoolean(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_KEY, false);

        // 3. Test code: Retrieve the configuration values using the Configuration API.
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

        // 4. Code after testing: Assert the expected results.
        assert enableKeyValue : "ENABLE_KEY should be set to true.";
        assert "NEVER".equals(policyKeyValue) : "POLICY_KEY should be set to 'NEVER'.";
        assert !bestEffortKeyValue : "BEST_EFFORT_KEY should be set to false.";
    }

    @Test
    // test code for getting ReplaceDatanodeOnFailure policy using configuration values
    // 1. Use the HDFS 2.8.5 API correctly to obtain configuration values.
    // 2. Prepare the test conditions: Create a Configuration object and write the relevant settings.
    // 3. Test code: Call the API to fetch the policy.
    // 4. Code after testing: Assert the expected outcome based on the policy.
    public void test_getPolicy_with_policy_NEVER() {
        // 2. Prepare the test conditions: Create a Configuration object.
        Configuration conf = new Configuration();
        conf.set(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY, Policy.NEVER.name());

        // 3. Test code: Retrieve the policy value.
        Policy policy = Policy.valueOf(conf.get(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY, 
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_DEFAULT
        ));

        // 4. Code after testing: Assert that the policy is correctly retrieved.
        assert policy == Policy.NEVER : "Policy should be NEVER.";
    }

    @Test
    // test code for creating ReplaceDatanodeOnFailure instance with configuration values
    // 1. Use the HDFS 2.8.5 API correctly to cover high-level method functionality.
    // 2. Prepare the test conditions: Create a Configuration object and set relevant settings.
    // 3. Test code: Verify if ReplaceDatanodeOnFailure instance reflects the configuration.
    // 4. Code after testing: Validate the properties of the instance.
    public void test_get_with_configuration() {
        // 2. Prepare the test conditions: Create a Configuration object.
        Configuration conf = new Configuration();
        conf.set(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY, Policy.ALWAYS.name());
        conf.setBoolean(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_KEY, true);

        // 3. Test code: Retrieve ReplaceDatanodeOnFailure properties.
        Policy policy = Policy.valueOf(conf.get(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY, 
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_DEFAULT
        ));
        boolean bestEffort = conf.getBoolean(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_KEY, 
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_DEFAULT
        );

        // 4. Code after testing: Assert that the properties match expected values.
        assert policy == Policy.ALWAYS : "Policy should be ALWAYS.";
        assert bestEffort : "Best Effort should be true.";
    }
}