package org.apache.hadoop.hdfs.protocol.datatransfer;

import org.apache.hadoop.conf.Configuration;       
import org.apache.hadoop.util.ReflectionUtils;       
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;       
import org.junit.Test;

public class TrustedChannelResolverTest {       
    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void test_getInstance_withInvalidClass() {
        // Create configuration instance and set invalid resolver class value
        Configuration conf = new Configuration();
        conf.set(HdfsClientConfigKeys.DFS_TRUSTEDCHANNEL_RESOLVER_CLASS, "InvalidTrustedChannelResolver");

        try {
            // Attempt to resolve and instantiate TrustedChannelResolver with invalid class
            TrustedChannelResolver resolver = TrustedChannelResolver.getInstance(conf);
        } catch (RuntimeException e) {
            // Verify the exception message to confirm failure due to invalid class resolution
            assert e.getMessage().contains("InvalidTrustedChannelResolver");  // Ensure it relates to invalid configuration
        }
    }

    @Test
    public void test_getInstance_withDefaultClass() {
        // Create configuration instance with default resolver class provided implicitly
        Configuration conf = new Configuration();

        try {
            // Resolve and instantiate TrustedChannelResolver using default class setting
            TrustedChannelResolver resolver = TrustedChannelResolver.getInstance(conf);
            // Validate the resolver instance is not null and adheres to expected class type
            assert resolver != null; 
            assert resolver.getClass().equals(TrustedChannelResolver.class);
        } catch (RuntimeException e) {
            // If an exception occurs, ensure no unresolved error for default behavior
            assert false : "Failed to instantiate the default TrustedChannelResolver.";
        }
    }
}