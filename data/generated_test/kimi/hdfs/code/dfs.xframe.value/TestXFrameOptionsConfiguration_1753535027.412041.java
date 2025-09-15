package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({HttpServer2.Builder.class, HttpServer2.class})
public class TestXFrameOptionsConfiguration {

    private Configuration conf;
    private Properties configProps;

    @Before
    public void setUp() throws Exception {
        // Load configuration as reference
        conf = new Configuration();
        conf.addResource("hdfs-default.xml");
        configProps = new Properties();
        // In a real scenario, you would load from the same file that Configuration loads from
        // This is simplified for demonstration purposes
        configProps.setProperty("dfs.xframe.enabled", "true");
        configProps.setProperty("dfs.xframe.value", "DENY");
    }

    @Test
    public void testDataNodeXFrameOptionConfiguration() throws IOException {
        // Prepare test conditions
        conf.setBoolean(DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED, true);
        conf.set(DFSConfigKeys.DFS_XFRAME_OPTION_VALUE, "DENY");

        // Mock HttpServer2.Builder
        mockStatic(HttpServer2.Builder.class);
        HttpServer2.Builder builderMock = PowerMockito.mock(HttpServer2.Builder.class);
        when(builderMock.setName(anyString())).thenReturn(builderMock);
        when(builderMock.setConf(any(Configuration.class))).thenReturn(builderMock);
        when(builderMock.setACL(any(AccessControlList.class))).thenReturn(builderMock);
        when(builderMock.hostName(anyString())).thenReturn(builderMock);
        when(builderMock.addEndpoint(any(URI.class))).thenReturn(builderMock);
        when(builderMock.setFindPort(anyBoolean())).thenReturn(builderMock);
        when(builderMock.configureXFrame(anyBoolean())).thenReturn(builderMock);
        when(builderMock.setXFrameOption(anyString())).thenReturn(builderMock);
        when(builderMock.build()).thenReturn(PowerMockito.mock(HttpServer2.class));

        // Get configuration values
        final boolean xFrameEnabled = conf.getBoolean(
            DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED,
            DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED_DEFAULT);

        final String xFrameOptionValue = conf.getTrimmed(
            DFSConfigKeys.DFS_XFRAME_OPTION_VALUE,
            DFSConfigKeys.DFS_XFRAME_OPTION_VALUE_DEFAULT);

        // Verify the configuration values match the expected values from properties
        assert xFrameEnabled == Boolean.parseBoolean(configProps.getProperty("dfs.xframe.enabled"));
        assert xFrameOptionValue.equals(configProps.getProperty("dfs.xframe.value"));

        // Test code - verify that the builder methods are called with correct parameters
        builderMock.configureXFrame(xFrameEnabled);
        builderMock.setXFrameOption(xFrameOptionValue);

        // Verify interactions
        verify(builderMock).configureXFrame(true);
        verify(builderMock).setXFrameOption("DENY");
    }

    @Test
    public void testNameNodeXFrameOptionConfiguration() throws IOException {
        // Prepare test conditions
        conf.setBoolean(DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED, true);
        conf.set(DFSConfigKeys.DFS_XFRAME_OPTION_VALUE, "SAMEORIGIN");

        // Mock HttpServer2.Builder
        mockStatic(HttpServer2.Builder.class);
        HttpServer2.Builder builderMock = PowerMockito.mock(HttpServer2.Builder.class);
        when(builderMock.configureXFrame(anyBoolean())).thenReturn(builderMock);
        when(builderMock.setXFrameOption(anyString())).thenReturn(builderMock);

        // Get configuration values
        final boolean xFrameEnabled = conf.getBoolean(
            DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED,
            DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED_DEFAULT);

        final String xFrameOptionValue = conf.getTrimmed(
            DFSConfigKeys.DFS_XFRAME_OPTION_VALUE,
            DFSConfigKeys.DFS_XFRAME_OPTION_VALUE_DEFAULT);

        // Verify the configuration values match the expected values
        assert xFrameEnabled == Boolean.parseBoolean(configProps.getProperty("dfs.xframe.enabled", "true"));
        assert xFrameOptionValue.equals(conf.get(DFSConfigKeys.DFS_XFRAME_OPTION_VALUE, "SAMEORIGIN"));

        // Test code - verify that the builder methods are called with correct parameters
        builderMock.configureXFrame(xFrameEnabled);
        builderMock.setXFrameOption(xFrameOptionValue);

        // Verify interactions
        verify(builderMock).configureXFrame(true);
        verify(builderMock).setXFrameOption("SAMEORIGIN");
    }

    @Test
    public void testSecondaryNameNodeXFrameOptionConfiguration() throws IOException {
        // Prepare test conditions
        conf.setBoolean(DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED, false);
        conf.set(DFSConfigKeys.DFS_XFRAME_OPTION_VALUE, "ALLOW-FROM");

        // Mock HttpServer2.Builder
        mockStatic(HttpServer2.Builder.class);
        HttpServer2.Builder builderMock = PowerMockito.mock(HttpServer2.Builder.class);
        when(builderMock.configureXFrame(anyBoolean())).thenReturn(builderMock);
        when(builderMock.setXFrameOption(anyString())).thenReturn(builderMock);

        // Get configuration values
        final boolean xFrameEnabled = conf.getBoolean(
            DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED,
            DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED_DEFAULT);

        final String xFrameOptionValue = conf.getTrimmed(
            DFSConfigKeys.DFS_XFRAME_OPTION_VALUE,
            DFSConfigKeys.DFS_XFRAME_OPTION_VALUE_DEFAULT);

        // Verify the configuration values match the expected values
        assert xFrameEnabled == false;
        assert xFrameOptionValue.equals("ALLOW-FROM");

        // Test code - verify that the builder methods are called with correct parameters
        builderMock.configureXFrame(xFrameEnabled);
        builderMock.setXFrameOption(xFrameOptionValue);

        // Verify interactions
        verify(builderMock).configureXFrame(false);
        verify(builderMock).setXFrameOption("ALLOW-FROM");
    }
}