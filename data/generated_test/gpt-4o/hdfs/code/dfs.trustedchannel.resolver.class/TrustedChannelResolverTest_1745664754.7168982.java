package org.apache.hadoop.hdfs.protocol.datatransfer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.Test;

public class TrustedChannelResolverTest {
    
    // Prepare the input conditions for unit testing.
    @Test
    public void test_getInstance_withInvalidClass() {
        Configuration conf = new Configuration();

        // Get configuration value using API
        String resolverClass = conf.get(HdfsClientConfigKeys.DFS_TRUSTEDCHANNEL_RESOLVER_CLASS, "InvalidTrustedChannelResolver");

        try {
            // Attempt to instantiate the TrustedChannelResolver with an invalid class
            TrustedChannelResolver resolver = TrustedChannelResolver.getInstance(conf);
        } catch (RuntimeException e) {
            // Verify that a RuntimeException is thrown
            assert e.getMessage().contains("InvalidTrustedChannelResolver");
        }
    }
}