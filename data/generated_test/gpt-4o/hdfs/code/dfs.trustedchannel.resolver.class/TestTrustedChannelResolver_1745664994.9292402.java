package org.apache.hadoop.hdfs.protocol.datatransfer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class TestTrustedChannelResolver {

    // Test for verifying behavior of getInstance across multiple invocations
    @Test
    public void test_getInstance_withMultipleInvocations() {
        // Get configuration values using API
        Configuration conf = new Configuration();
        conf.setClass(
            "dfs.trustedchannel.resolver.class",
            TrustedChannelResolver.class,
            TrustedChannelResolver.class
        );

        // Prepare the input conditions for unit testing
        TrustedChannelResolver resolverInstance1 = TrustedChannelResolver.getInstance(conf);
        TrustedChannelResolver resolverInstance2 = TrustedChannelResolver.getInstance(conf);

        // Test code - Assert that different instances are created across multiple calls
        assertNotSame(resolverInstance1, resolverInstance2);
        assertNotNull(resolverInstance1);
        assertNotNull(resolverInstance2);
    }

    // Test for verifying correct instance creation using configuration
    @Test
    public void test_getInstance_resolverConfigurationUsage() {
        // Get configuration values using API
        Configuration conf = new Configuration();
        conf.setClass(
            "dfs.trustedchannel.resolver.class",
            TrustedChannelResolver.class,
            TrustedChannelResolver.class
        );

        // Prepare the input conditions for unit testing
        TrustedChannelResolver resolverInstance = TrustedChannelResolver.getInstance(conf);

        // Test code - Verify instance creation and configuration
        assertNotNull(resolverInstance);
        assertTrue(resolverInstance instanceof TrustedChannelResolver);
    }

    // Test for verifying ReflectionUtils usage to instantiate TrustedChannelResolver
    @Test
    public void test_ReflectionUtils_newInstance() {
        // Get configuration values using API
        Configuration conf = new Configuration();
        conf.setClass(
            "dfs.trustedchannel.resolver.class",
            TrustedChannelResolver.class,
            TrustedChannelResolver.class
        );

        // Prepare the input conditions for unit testing
        Class<? extends TrustedChannelResolver> clazz = conf.getClass(
            "dfs.trustedchannel.resolver.class",
            TrustedChannelResolver.class,
            TrustedChannelResolver.class
        );
        TrustedChannelResolver resolverInstance = ReflectionUtils.newInstance(clazz, conf);

        // Test code - Verify that ReflectionUtils successfully creates an instance
        assertNotNull(resolverInstance);
        assertTrue(resolverInstance instanceof TrustedChannelResolver);
    }
}