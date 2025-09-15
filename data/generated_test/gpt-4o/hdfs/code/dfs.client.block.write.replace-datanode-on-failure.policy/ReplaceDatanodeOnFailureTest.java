package org.apache.hadoop.hdfs.protocol.datatransfer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.Test;
import static org.junit.Assert.*;

public class ReplaceDatanodeOnFailureTest {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_write_with_policy_ALWAYS() {
        // Prepare the test conditions
        Configuration conf = new Configuration();

        // Test the write method
        ReplaceDatanodeOnFailure.write(
            ReplaceDatanodeOnFailure.Policy.ALWAYS,
            true, // bestEffort
            conf
        );

        // Assert the ENABLE_KEY is correctly set
        assertTrue(conf.getBoolean(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY,
            false
        ));

        // Assert the POLICY_KEY is correctly set to "ALWAYS"
        assertEquals(
            "ALWAYS",
            conf.get(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY)
        );

        // Assert the BEST_EFFORT_KEY is correctly set to true
        assertTrue(conf.getBoolean(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_KEY,
            false
        ));
    }
}