package org.apache.hadoop.hdfs.protocol.datatransfer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import static org.junit.Assert.*;

public class TrustedChannelResolverTest {

    @Test
    // Ensure `getInstance` throws a runtime exception when an invalid class name 
    // is set for `dfs.trustedchannel.resolver.class` in the configuration.
    // 1. You need to use the HDFS 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getInstance_withInvalidResolverClass() {
        // 1. Prepare the test condition: Create a Configuration object
        Configuration conf = new Configuration();

        // 2. Specify a non-existent class name in `dfs.trustedchannel.resolver.class`
        conf.set(HdfsClientConfigKeys.DFS_TRUSTEDCHANNEL_RESOLVER_CLASS, "com.example.NonExistentClass");

        try {
            // 3. Attempt to invoke `TrustedChannelResolver.getInstance(conf)` and capture exceptions
            TrustedChannelResolver resolver = TrustedChannelResolver.getInstance(conf);

            // 4. Testing code: The test should fail if no exception is thrown
            fail("Expected RuntimeException due to invalid class name, but no exception was thrown.");
        } catch (RuntimeException e) {
            // 4. Code after testing: Assert that the exception message mentions the invalid class name
            assertTrue("Exception message should contain the invalid class name.",
                    e.getMessage().contains("com.example.NonExistentClass"));
        }
    }

    @Test
    // Ensure `getInstance` successfully resolves the default `TrustedChannelResolver` class when no custom resolver class is specified in the configuration.
    // 1. You need to use the HDFS 2.8.5 API to ensure fallback to the default resolver.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getInstance_withDefaultResolverClass() {
        // 1. Prepare the test condition: Create a Configuration object with default settings
        Configuration conf = new Configuration();

        try {
            // 2. Invoke `TrustedChannelResolver.getInstance(conf)`
            TrustedChannelResolver resolver = TrustedChannelResolver.getInstance(conf);

            // 3. Test code: Assert that the resolved instance is of the default class
            assertEquals("Resolved class should be the default TrustedChannelResolver.",
                    TrustedChannelResolver.class, resolver.getClass());
        } catch (RuntimeException e) {
            // 4. Code after testing: The test should fail if any unexpected exception is thrown
            fail("Unexpected exception thrown: " + e.getMessage());
        }
    }
}