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

import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ReflectionUtils.class, TrustedChannelResolver.class})
public class TrustedChannelResolverConfigTest {

    private Configuration conf;
    private Properties configProperties;

    @Before
    public void setUp() throws Exception {
        conf = new Configuration();
        configProperties = new Properties();
        InputStream input = this.getClass().getClassLoader().getResourceAsStream("core-site.xml");
        if (input != null) {
            configProperties.loadFromXML(input);
        }

        PowerMockito.mockStatic(ReflectionUtils.class);
    }

    @Test
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
    public void testGetInstanceUsesCustomClassWhenConfigured() {
        // Prepare
        String customClassName = "org.apache.hadoop.hdfs.protocol.datatransfer.CustomTrustedChannelResolver";
        conf.setClass(HdfsClientConfigKeys.DFS_TRUSTEDCHANNEL_RESOLVER_CLASS, CustomTrustedChannelResolver.class, TrustedChannelResolver.class);
        
        CustomTrustedChannelResolver mockCustomResolver = mock(CustomTrustedChannelResolver.class);
        PowerMockito.when(ReflectionUtils.newInstance(CustomTrustedChannelResolver.class, conf))
                .thenReturn(mockCustomResolver);

        // Test
        TrustedChannelResolver result = TrustedChannelResolver.getInstance(conf);

        // Verify
        assertEquals(mockCustomResolver, result);
        PowerMockito.verifyStatic();
        ReflectionUtils.newInstance(CustomTrustedChannelResolver.class, conf);
    }

    @Test
    public void testConfigurationValueMatchesPropertiesFile() {
        // Get value from Configuration API
        String configValue = conf.get(HdfsClientConfigKeys.DFS_TRUSTEDCHANNEL_RESOLVER_CLASS);
        
        // Get value from properties file
        String propertiesValue = configProperties.getProperty(HdfsClientConfigKeys.DFS_TRUSTEDCHANNEL_RESOLVER_CLASS);
        
        // Compare
        assertEquals("Configuration value should match properties file", propertiesValue, configValue);
    }

    // Custom resolver for testing purposes
    public static class CustomTrustedChannelResolver extends TrustedChannelResolver {
        @Override
        public boolean isTrusted() {
            return false;
        }
    }
}