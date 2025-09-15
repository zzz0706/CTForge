package org.apache.hadoop.hdfs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.Assert;
import org.junit.Test;

public class TestConfigurationValidity {

    @Test
    public void testTrustedChannelResolverConfigurationValidity() {
        // Step 1: Load configuration
        Configuration conf = new Configuration();

        // Step 2: Read the configuration value for dfs.trustedchannel.resolver.class
        String resolverClassConfig = conf.get(HdfsClientConfigKeys.DFS_TRUSTEDCHANNEL_RESOLVER_CLASS, null);
        
        // Step 3: Validate the configuration value
        try {
            // The configuration value should be either empty or a valid class that can be instantiated.
            if (resolverClassConfig != null && !resolverClassConfig.trim().isEmpty()) {
                // Attempt to load the class
                Class<?> resolverClass = conf.getClass(HdfsClientConfigKeys.DFS_TRUSTEDCHANNEL_RESOLVER_CLASS, TrustedChannelResolver.class);

                // Ensure the loaded class is of the expected type
                Assert.assertTrue("Class specified in dfs.trustedchannel.resolver.class is not a subclass of TrustedChannelResolver", 
                    TrustedChannelResolver.class.isAssignableFrom(resolverClass));

                // Check if the class can be instantiated
                Object instance = TrustedChannelResolver.getInstance(conf);
                Assert.assertNotNull("Failed to instantiate dfs.trustedchannel.resolver.class", instance);
            } else {
                // If the configuration value is empty, it defaults to TrustedChannelResolver
                Object defaultInstance = TrustedChannelResolver.getInstance(conf);
                Assert.assertNotNull("Default implementation of TrustedChannelResolver cannot be instantiated", defaultInstance);
            }

        } catch (Exception e) {
            Assert.fail("Invalid configuration value for dfs.trustedchannel.resolver.class: " + e.getMessage());
        }
    }
}