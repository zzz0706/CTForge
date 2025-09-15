package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.Assert;
import org.junit.Test;
//hdfs-5872

public class HttpPolicyConfigValueValidationTest {

    private static final String KEY = DFSConfigKeys.DFS_HTTP_POLICY_KEY;

    private boolean isValidHttpPolicy(String policy) {
        if (policy == null) {
            return false;
        }
        try {
            HttpConfig.Policy.valueOf(policy);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    @Test
    public void testValidHttpPolicyValues() {
        Configuration conf = new HdfsConfiguration();
        for (HttpConfig.Policy p : HttpConfig.Policy.values()) {
            String policy = conf.get(KEY);
            Assert.assertTrue(
                "configuration " + KEY + "='" + policy,
                isValidHttpPolicy(policy)
            );
        }
    }
}
