package org.apache.hadoop.hdfs.protocol.datatransfer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

public class TrustedChannelResolverTest {

    // Test case to verify behavior when an invalid class is specified
    // Prepare the input conditions for unit testing.
    @Test
    public void test_getInstance_withInvalidClass() {
        // Create configuration instance and set an invalid resolver class
        Configuration conf = new Configuration();
        conf.set(HdfsClientConfigKeys.DFS_TRUSTEDCHANNEL_RESOLVER_CLASS, "InvalidTrustedChannelResolver");

        try {
            // Attempting instantiation should throw a RuntimeException
            TrustedChannelResolver.getInstance(conf);
            assert false : "Expected RuntimeException due to invalid class resolution.";
        } catch (RuntimeException e) {
            // Verify the exception to confirm failure due to invalid class name
            assert e.getMessage().contains("InvalidTrustedChannelResolver");
        }
    }

    // Test case to verify behavior when default resolver class is used
    // Prepare the input conditions for unit testing.
    @Test
    public void test_getInstance_withDefaultClass() {
        // Create configuration without overriding the default resolver class
        Configuration conf = new Configuration();

        try {
            // Resolve and validate TrustedChannelResolver instance
            TrustedChannelResolver resolver = TrustedChannelResolver.getInstance(conf);
            assert resolver != null;
            assert resolver.getClass().equals(TrustedChannelResolver.class);
        } catch (RuntimeException e) {
            // Ensure no exceptions occur while resolving the default instance
            assert false : "Exception should not occur with default TrustedChannelResolver.";
        }
    }

    // Test case for validating ReflectionUtils-based instantiation
    // Prepare the input conditions for unit testing.
    @Test
    public void test_getInstance_withCustomResolverClass() {
        // Create configuration and set custom resolver class
        Configuration conf = new Configuration();
        conf.set(HdfsClientConfigKeys.DFS_TRUSTEDCHANNEL_RESOLVER_CLASS, TrustedChannelResolver.class.getName());

        try {
            // Resolve resolver class and instantiate using ReflectionUtils
            Class<? extends TrustedChannelResolver> resolverClass = conf.getClass(
                HdfsClientConfigKeys.DFS_TRUSTEDCHANNEL_RESOLVER_CLASS, TrustedChannelResolver.class)
                .asSubclass(TrustedChannelResolver.class);
            TrustedChannelResolver resolverInstance = ReflectionUtils.newInstance(resolverClass, conf);

            // Validate the instantiated resolver
            assert resolverInstance != null;
            assert resolverInstance.getClass().equals(TrustedChannelResolver.class);
        } catch (RuntimeException e) {
            // Ensure no exceptions occur while instantiating the resolver
            assert false : "Exception should not occur with valid TrustedChannelResolver.";
        }
    }
}