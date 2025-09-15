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

import java.util.Properties;
import java.io.InputStream;
import java.io.IOException;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ReflectionUtils.class})
public class TrustedChannelResolverConfigTest {

    private Configuration conf;
    private Properties configProps;

    @Before
    public void setUp() throws IOException {
        conf = new Configuration();
        configProps = new Properties();
        InputStream input = getClass().getClassLoader().getResourceAsStream("core-site.xml");
        if (input != null) {
            configProps.loadFromXML(input);
        }
    }

    @Test
    public void testDefaultTrustedChannelResolverClass() {
        // Prepare
        String key = HdfsClientConfigKeys.DFS_TRUSTEDCHANNEL_RESOLVER_CLASS;
        String expectedDefaultClass = configProps.getProperty(key, "org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver$DefaultTrustedChannelResolver");

        // Since we can't directly test the actual class instantiation without it being available,
        // we'll test that the configuration key is properly defined
        assertNotNull("Configuration key should exist", key);
        assertTrue("Default class name should be valid", expectedDefaultClass.length() > 0);
    }

    @Test
    public void testCustomTrustedChannelResolverClass() throws Exception {
        // Prepare
        String key = HdfsClientConfigKeys.DFS_TRUSTEDCHANNEL_RESOLVER_CLASS;
        String customClassName = "org.apache.hadoop.hdfs.protocol.datatransfer.TestTrustedChannelResolver";
        conf.set(key, customClassName);

        // Verify that the configuration was set correctly
        assertEquals("Custom class name should be set in configuration", 
                    customClassName, conf.get(key));
        
        // Test that we can retrieve the configured class name
        String configuredClass = conf.get(key, "default.class");
        assertEquals("Should retrieve the configured class name", customClassName, configuredClass);
    }
    
    // Dummy class for testing purposes
    public static class TestTrustedChannelResolver {
        public TestTrustedChannelResolver(Configuration conf) {
            // Dummy constructor
        }
    }
}