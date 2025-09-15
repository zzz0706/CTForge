package org.apache.hadoop.hdfs.protocol.datatransfer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

public class TrustedChannelResolverTest {

    // Test case to verify the behavior when an invalid class is specified
    // Prepare the input conditions for unit testing.
    @Test
    public void test_getInstance_withInvalidClass() {
        // Create configuration instance and set invalid resolver class value
        Configuration conf = new Configuration();
        conf.set(HdfsClientConfigKeys.DFS_TRUSTEDCHANNEL_RESOLVER_CLASS, "InvalidTrustedChannelResolver");

        try {
            // Attempt to resolve and instantiate TrustedChannelResolver with invalid class
            TrustedChannelResolver.getInstance(conf);
            assert false : "Expected RuntimeException due to invalid class resolution.";
        } catch (RuntimeException e) {
            // Verify the exception message to confirm failure due to invalid class resolution
            assert e.getMessage().contains("InvalidTrustedChannelResolver");
        }
    }

    // Test case to verify the behavior with default class
    // Prepare the input conditions for unit testing.
    @Test
    public void test_getInstance_withDefaultClass() {
        // Create configuration instance with default resolver class provided implicitly
        Configuration conf = new Configuration();

        try {
            // Resolve and instantiate TrustedChannelResolver using default class setting
            TrustedChannelResolver resolver = TrustedChannelResolver.getInstance(conf);
            // Validate resolver instance
            assert resolver != null;
            assert resolver.getClass().equals(TrustedChannelResolver.class);
        } catch (RuntimeException e) {
            // If an exception occurs, log test failure
            assert false : "Failed to instantiate the default TrustedChannelResolver.";
        }
    }

    // Test case to verify proper instantiation using ReflectionUtils
    // Prepare the input conditions for unit testing.
    @Test
    public void test_newInstance_withReflectionUtils() {
        // Prepare test conditions by setting configuration with resolver class
        Configuration conf = new Configuration();
        conf.set(HdfsClientConfigKeys.DFS_TRUSTEDCHANNEL_RESOLVER_CLASS, TrustedChannelResolver.class.getName());

        try {
            // Resolve resolver class using Configuration API
            Class<? extends TrustedChannelResolver> resolverClass = conf.getClass(
                HdfsClientConfigKeys.DFS_TRUSTEDCHANNEL_RESOLVER_CLASS, TrustedChannelResolver.class)
                .asSubclass(TrustedChannelResolver.class); // Ensure proper type

            // Instantiate using ReflectionUtils
            TrustedChannelResolver resolverInstance = ReflectionUtils.newInstance(resolverClass, conf);

            // Validate resolver instance
            assert resolverInstance != null;
            assert resolverInstance.getClass().equals(TrustedChannelResolver.class);
        } catch (RuntimeException e) {
            // If an exception occurs, log test failure
            assert false : "Failed to instantiate TrustedChannelResolver using ReflectionUtils.";
        }
    }
}