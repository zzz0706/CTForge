package org.apache.hadoop.hdfs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.datatransfer.ReplaceDatanodeOnFailure;
import org.apache.hadoop.hdfs.protocol.datatransfer.ReplaceDatanodeOnFailure.Policy;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestReplaceDatanodeOnFailureConfiguration {
    // get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void testWritePolicyConfiguration() {
        // Step 1: Create a Configuration instance.
        Configuration conf = new Configuration();

        // Step 2: Call ReplaceDatanodeOnFailure.write(Policy.DEFAULT, true, Configuration instance).
        ReplaceDatanodeOnFailure.write(Policy.DEFAULT, true, conf);

        // Step 3: Fetch and validate the values of the related configuration keys.
        boolean enableKey = conf.getBoolean(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY, 
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_DEFAULT);
        String policyKey = conf.get(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY, 
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_DEFAULT);
        boolean bestEffortKey = conf.getBoolean(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_KEY, 
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_DEFAULT);

        // Step 4: Assert the expected results.
        assertTrue("ENABLE_KEY should be true.", enableKey);
        assertEquals("POLICY_KEY should be 'DEFAULT'.", "DEFAULT", policyKey);
        assertTrue("BEST_EFFORT_KEY should be true.", bestEffortKey);
    }
}