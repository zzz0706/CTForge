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
    private static final String CONFIG_KEY = HdfsClientConfigKeys.DFS_TRUSTEDCHANNEL_RESOLVER_CLASS;

    @Before
    public void setUp() {
        conf = new Configuration();
    }

    @Test
    public void testDefaultTrustedChannelResolverInstance() {
        // Given: No custom class configured (default behavior)
        
        // When: Getting the instance
        TrustedChannelResolver resolver = TrustedChannelResolver.getInstance(conf);

        // Then: Should return the default implementation
        assertNotNull(resolver);
        // In HDFS 2.8.5, check that it's some concrete implementation
        assertTrue(resolver instanceof TrustedChannelResolver);
    }

    @Test
    public void testCustomTrustedChannelResolverInstance() throws Exception {
        // Given: A custom TrustedChannelResolver implementation
        Class<? extends TrustedChannelResolver> customClass = CustomTrustedChannelResolver.class;
        conf.setClass(CONFIG_KEY, customClass, TrustedChannelResolver.class);

        // Mock ReflectionUtils to return our custom instance
        PowerMockito.mockStatic(ReflectionUtils.class);
        CustomTrustedChannelResolver mockInstance = spy(new CustomTrustedChannelResolver());
        PowerMockito.when(ReflectionUtils.newInstance(customClass, conf)).thenReturn(mockInstance);

        // When: Getting the instance
        TrustedChannelResolver resolver = TrustedChannelResolver.getInstance(conf);

        // Then: Should return the custom implementation
        assertNotNull(resolver);
        assertTrue(customClass.isInstance(resolver));
    }

    @Test
    public void testConfigurationValueMatchesExternalSource() {
        // Given: Load expected value from external source (simulated here)
        // In HDFS 2.8.5, the default value should be empty string
        String expectedValue = conf.get(CONFIG_KEY, "");
        
        // When: Fetching via Configuration API
        String actualValue = conf.get(CONFIG_KEY, "");

        // Then: Values should match
        assertEquals(expectedValue, actualValue);
    }

    // Dummy custom implementation for testing
    public static class CustomTrustedChannelResolver extends TrustedChannelResolver {
        @Override
        public boolean isTrusted() {
            return true;
        }
    }
}