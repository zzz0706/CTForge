package org.apache.hadoop.hdfs.protocol.datatransfer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestTrustedChannelResolver {

    // Test for verifying behavior of getInstance using configuration
    @Test
    public void test_getInstance_withConfiguration() {
        // Get configuration values using API
        Configuration conf = new Configuration();
        conf.setClass(
            "dfs.trustedchannel.resolver.class",
            TrustedChannelResolver.class,
            TrustedChannelResolver.class
        );

        // Prepare the input conditions for unit testing
        TrustedChannelResolver resolverInstance = TrustedChannelResolver.getInstance(conf);

        // Test code - Verify correct instance creation using configuration
        assertNotNull(resolverInstance);
        assertTrue(resolverInstance instanceof TrustedChannelResolver);
    }

    // Test for verifying if getInstance correctly initializes different instances for calls
    @Test
    public void test_getInstance_multipleCalls() {
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

        // Test code - Verify different instances are created via multiple calls
        assertNotNull(resolverInstance1);
        assertNotNull(resolverInstance2);
    }
    
    // Test for verifying ReflectionUtils instantiates TrustedChannelResolver correctly
    @Test
    public void test_ReflectionUtils_instanceCreation() {
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

        // Test code - Ensure correct instantiation with ReflectionUtils
        assertNotNull(resolverInstance);
        assertTrue(resolverInstance instanceof TrustedChannelResolver);
    }
}