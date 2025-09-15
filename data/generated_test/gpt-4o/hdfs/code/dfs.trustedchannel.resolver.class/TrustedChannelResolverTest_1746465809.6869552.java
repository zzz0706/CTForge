package org.apache.hadoop.hdfs.protocol.datatransfer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.junit.Test;

public class TrustedChannelResolverTest {

    @Test
    // Ensure `getInstance` throws a runtime exception when an invalid class name 
    // is set for `dfs.trustedchannel.resolver.class` in the configuration.
    // 1. You need to use the HDFS 2.8.5 API correctly to obtain configuration values,
    //    instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getInstance_withInvalidResolverClass() {
        // Prepare the test condition: Create a Configuration object
        Configuration conf = new Configuration();

        // Specify a non-existent class name in `dfs.trustedchannel.resolver.class`
        conf.set(HdfsClientConfigKeys.DFS_TRUSTEDCHANNEL_RESOLVER_CLASS, "com.example.NonExistentClass");

        try {
            // Attempt to invoke `TrustedChannelResolver.getInstance(conf)` and capture exceptions
            TrustedChannelResolver resolver = TrustedChannelResolver.getInstance(conf);
            // The code should not reach here if the resolver fails to instantiate
            assert false : "RuntimeException expected but not thrown.";
        } catch (RuntimeException e) {
            // Test code: Assert that the exception message mentions the invalid class name
            assert e.getMessage().contains("com.example.NonExistentClass") : "Exception message does not contain expected class name.";
        }
    }

    @Test
    // Ensure `getInstance` successfully resolves the default `TrustedChannelResolver` class when no custom resolver class is specified in the configuration.
    // 1. Use the HDFS 2.8.5 API to ensure fallback to the default resolver.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getInstance_withDefaultResolverClass() {
        // Prepare the test condition: Create a Configuration object with no resolver class set
        Configuration conf = new Configuration();

        try {
            // Invoke `TrustedChannelResolver.getInstance(conf)`
            TrustedChannelResolver resolver = TrustedChannelResolver.getInstance(conf);

            // Test code: Assert that the resolved instance is of the default class
            assert resolver.getClass().equals(TrustedChannelResolver.class) : "Resolved class is not the default TrustedChannelResolver.";
        } catch (RuntimeException e) {
            // This test should not throw a RuntimeException
            assert false : "Unexpected exception thrown: " + e.getMessage();
        }
    }
}