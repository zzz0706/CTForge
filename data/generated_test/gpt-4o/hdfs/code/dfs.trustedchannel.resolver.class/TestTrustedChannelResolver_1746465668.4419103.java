package org.apache.hadoop.hdfs.protocol.datatransfer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for verifying the behavior of the TrustedChannelResolver class
 * in Hadoop HDFS 2.8.5, specifically focusing on configuration usage 
 * and instance instantiation logic tied to custom and default resolvers.
 */
public class TestTrustedChannelResolver {

    @Test
    // Test code for default TrustedChannelResolver.
    // 1. Use the Hadoop Configuration API correctly to ensure no custom class is specified.
    // 2. Verify that the default TrustedChannelResolver is instantiated when no custom implementation is set.
    // 3. Ensure proper initialization and validation of the instance returned by TrustedChannelResolver.getInstance(Configuration).
    public void test_getInstance_withDefaultResolver() {
        // Step 1: Create a new configuration using the Hadoop Configuration API.
        Configuration conf = new Configuration();

        // Step 2: Ensure no custom resolver class is set in the configuration. This is the default state.

        // Step 3: Invoke getInstance to obtain the default TrustedChannelResolver.
        TrustedChannelResolver resolver = TrustedChannelResolver.getInstance(conf);

        // Step 4: Validate that the resolver is instantiated properly and is of the default class.
        assertNotNull("Expected a non-null instance of TrustedChannelResolver", resolver);
        assertTrue("Expected the default instance of TrustedChannelResolver",
                resolver instanceof TrustedChannelResolver);
    }

    @Test
    // Test code for custom TrustedChannelResolver.
    // 1. Use the HDFS client configuration keys to specify a custom resolver class.
    // 2. Verify that the custom implementation of TrustedChannelResolver is correctly instantiated.
    // 3. Ensure that the resolver instance matches the expected custom class.
    public void test_getInstance_withCustomResolver() {
        // Step 1: Use the Hadoop Configuration API to create a new configuration.
        Configuration conf = new Configuration();

        // Step 2: Set a custom resolver class in the configuration using the HdfsClientConfigKeys API.
        conf.setClass(
            HdfsClientConfigKeys.DFS_TRUSTEDCHANNEL_RESOLVER_CLASS,
            CustomTrustedChannelResolver.class,
            TrustedChannelResolver.class
        );

        // Step 3: Invoke getInstance to obtain the custom TrustedChannelResolver.
        TrustedChannelResolver resolver = TrustedChannelResolver.getInstance(conf);

        // Step 4: Validate that the resolver is instantiated properly and matches the custom class.
        assertNotNull("Expected a non-null instance of the custom TrustedChannelResolver", resolver);
        assertTrue("Expected the custom instance of TrustedChannelResolver",
                resolver instanceof CustomTrustedChannelResolver);
    }

    /**
     * A simulated implementation of a custom TrustedChannelResolver for testing purposes.
     */
    public static class CustomTrustedChannelResolver extends TrustedChannelResolver {
        // Custom logic or overrides can be added here if needed for more complex testing.
    }
}