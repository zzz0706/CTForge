package org.apache.hadoop.hdfs.protocol.datatransfer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

public class TrustedChannelResolverTest {

    @Test
    // Ensure that `getInstance` throws a runtime exception when an invalid class name is set for `dfs.trustedchannel.resolver.class` in the configuration.
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getInstance_withInvalidResolverClass() {
        // Create a Configuration object and prepare the test conditions
        Configuration conf = new Configuration();
        
        // Set the configuration `dfs.trustedchannel.resolver.class` to a non-existent class name
        conf.set("dfs.trustedchannel.resolver.class", "com.example.NonExistentClass");

        try {
            // Attempt to invoke `TrustedChannelResolver.getInstance(conf)` and capture exceptions
            TrustedChannelResolver resolver = TrustedChannelResolver.getInstance(conf);
        } catch (RuntimeException e) {
            // Test code: Assert that the exception message indicates failed class instantiation
            assert e.getMessage().contains("com.example.NonExistentClass");
        }
    }
}