package org.apache.hadoop.hdfs.protocol.datatransfer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ReflectionUtils.class, TrustedChannelResolver.class})
public class TrustedChannelResolverConfigTest {

    private Configuration conf;

    @Before
    public void setUp() {
        conf = new Configuration();
    }

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testCustomTrustedChannelResolverInstantiation() throws Exception {
        // Prepare
        String customClassName = TestTrustedChannelResolver.class.getName();
        conf.set(HdfsClientConfigKeys.DFS_TRUSTEDCHANNEL_RESOLVER_CLASS, customClassName);

        // Test
        TrustedChannelResolver resolver = TrustedChannelResolver.getInstance(conf);

        // Verify
        assertNotNull("Resolver should not be null", resolver);
        assertTrue("Resolver should be instance of our custom class", 
                   resolver instanceof TestTrustedChannelResolver);
    }

    // Custom TrustedChannelResolver implementation for testing
    public static class TestTrustedChannelResolver extends TrustedChannelResolver {
        public TestTrustedChannelResolver() {
            // Default constructor required by ReflectionUtils
        }
        
        @Override
        public boolean isTrusted() {
            return true;
        }
    }
}