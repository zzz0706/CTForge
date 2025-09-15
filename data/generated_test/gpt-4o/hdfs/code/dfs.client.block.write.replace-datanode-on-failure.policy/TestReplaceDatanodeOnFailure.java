package org.apache.hadoop.hdfs.protocol.datatransfer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.datatransfer.ReplaceDatanodeOnFailure.Policy;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestReplaceDatanodeOnFailure {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_get_with_invalid_policy() {
        // 1. Prepare the Configuration object and set up invalid policy value
        Configuration conf = new Configuration();
        conf.setBoolean(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY, true);
        conf.set(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY, "INVALID_POLICY");

        try {
            // 2. Access the policy via the Policy#valueOf method since the original issue lies in the incorrect method call
            Policy policy = Policy.valueOf(conf.get(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY));
            
            // 3. Assert fail because the method should throw an exception for invalid values
            fail("Expected IllegalArgumentException to be thrown for invalid policy value.");
        } catch (IllegalArgumentException e) {
            // 4. Assert that the exception contains the expected error message
            assertTrue(e.getMessage().contains("No enum constant"));
        }
    }
}