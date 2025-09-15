package org.apache.hadoop.hdfs.protocol.datatransfer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class TestTrustedChannelResolver {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getInstance_withDefaultResolver() {
        // Step 1: Create a Configuration object without setting the dfs.trustedchannel.resolver.class property.
        Configuration conf = new Configuration();

        // Step 2: Invoke TrustedChannelResolver.getInstance(conf) to get the default resolver instance.
        TrustedChannelResolver resolver = TrustedChannelResolver.getInstance(conf);

        // Step 3: Verify the returned object is an instance of the default TrustedChannelResolver class.
        assertTrue("Expected instance of default TrustedChannelResolver class", resolver instanceof TrustedChannelResolver);

        // Code after testing: Cleanup or reset configuration if necessary; 
        // here we don't have additional teardown for this specific test case.
    }
}