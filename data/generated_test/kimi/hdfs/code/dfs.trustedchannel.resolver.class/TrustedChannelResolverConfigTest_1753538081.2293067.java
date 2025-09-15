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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ReflectionUtils.class, TrustedChannelResolver.class})
public class TrustedChannelResolverConfigTest {

    private Configuration conf;

    @Before
    public void setUp() throws Exception {
        conf = new Configuration();
        PowerMockito.mockStatic(ReflectionUtils.class);
    }

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testGetInstanceUsesDefaultWhenConfigNotSet() {
        // Prepare
        TrustedChannelResolver mockDefaultResolver = mock(TrustedChannelResolver.class);
        PowerMockito.when(ReflectionUtils.newInstance(TrustedChannelResolver.class, conf))
                .thenReturn(mockDefaultResolver);

        // Test
        TrustedChannelResolver result = TrustedChannelResolver.getInstance(conf);

        // Verify
        assertEquals(mockDefaultResolver, result);
        PowerMockito.verifyStatic();
        ReflectionUtils.newInstance(TrustedChannelResolver.class, conf);
    }

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testGetInstanceUsesCustomClassWhenConfigured() {
        // Prepare
        Class<? extends TrustedChannelResolver> customClass = CustomTrustedChannelResolver.class;
        conf.setClass(HdfsClientConfigKeys.DFS_TRUSTEDCHANNEL_RESOLVER_CLASS, customClass, TrustedChannelResolver.class);
        
        CustomTrustedChannelResolver mockCustomResolver = mock(CustomTrustedChannelResolver.class);
        PowerMockito.when(ReflectionUtils.newInstance(customClass, conf))
                .thenReturn(mockCustomResolver);

        // Test
        TrustedChannelResolver result = TrustedChannelResolver.getInstance(conf);

        // Verify
        assertEquals(mockCustomResolver, result);
        PowerMockito.verifyStatic();
        ReflectionUtils.newInstance(customClass, conf);
    }

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testTrustedChannelResolverCaching() throws Exception {
        // Prepare: Set up configuration with custom resolver class
        Class<? extends TrustedChannelResolver> customClass = CustomTrustedChannelResolver.class;
        conf.setClass(HdfsClientConfigKeys.DFS_TRUSTEDCHANNEL_RESOLVER_CLASS, 
                      customClass, 
                      TrustedChannelResolver.class);
        
        // Mock the ReflectionUtils to return the same instance
        CustomTrustedChannelResolver mockResolver = mock(CustomTrustedChannelResolver.class);
        PowerMockito.when(ReflectionUtils.newInstance(customClass, conf))
                .thenReturn(mockResolver);

        // Create two instances to test caching
        TrustedChannelResolver firstInstance = TrustedChannelResolver.getInstance(conf);
        TrustedChannelResolver secondInstance = TrustedChannelResolver.getInstance(conf);
        
        // Verify that we get the same instance due to caching
        assertSame(firstInstance, secondInstance);
        assertEquals(mockResolver, firstInstance);
        assertEquals(mockResolver, secondInstance);
    }

    // Custom resolver for testing purposes
    public static class CustomTrustedChannelResolver extends TrustedChannelResolver {
        @Override
        public boolean isTrusted() {
            return false;
        }
    }
}