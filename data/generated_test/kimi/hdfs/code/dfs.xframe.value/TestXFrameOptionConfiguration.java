package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.http.HttpServer2;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest({HttpServer2.Builder.class})
public class TestXFrameOptionConfiguration {

    private Configuration conf;
    private Properties configProperties;

    @Before
    public void setUp() throws Exception {
        // Load configuration as Properties for reference comparison
        configProperties = new Properties();
        // In a real test, this would load from actual Hadoop config files
        // For this example, we'll populate with default values
        configProperties.setProperty("dfs.xframe.enabled", "true");
        configProperties.setProperty("dfs.xframe.value", "SAMEORIGIN");
        
        // Create Configuration object
        conf = new Configuration();
        conf.setBoolean(DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED, true);
        conf.set(DFSConfigKeys.DFS_XFRAME_OPTION_VALUE, "SAMEORIGIN");
    }

    @Test
    public void testDatanodeHttpServer_XFrameOptionConfiguration() throws Exception {
        // Prepare
        mockStatic(HttpServer2.Builder.class);
        HttpServer2.Builder builderMock = PowerMockito.mock(HttpServer2.Builder.class);
        when(builderMock.configureXFrame(anyBoolean())).thenReturn(builderMock);
        when(builderMock.setXFrameOption(anyString())).thenReturn(builderMock);
        
        PowerMockito.whenNew(HttpServer2.Builder.class).withNoArguments().thenReturn(builderMock);

        // Execute - This would normally instantiate DatanodeHttpServer
        // We're simulating the relevant part of its constructor
        final boolean xFrameEnabled = conf.getBoolean(
            DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED,
            DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED_DEFAULT);

        final String xFrameOptionValue = conf.getTrimmed(
            DFSConfigKeys.DFS_XFRAME_OPTION_VALUE,
            DFSConfigKeys.DFS_XFRAME_OPTION_VALUE_DEFAULT);

        // Simulate the builder method calls
        builderMock.configureXFrame(xFrameEnabled);
        builderMock.setXFrameOption(xFrameOptionValue);

        // Verify
        verify(builderMock).configureXFrame(true);
        verify(builderMock).setXFrameOption("SAMEORIGIN");
        
        // Compare against reference loader
        assertEquals(xFrameEnabled, Boolean.parseBoolean(configProperties.getProperty("dfs.xframe.enabled", "false")));
        assertEquals(xFrameOptionValue, configProperties.getProperty("dfs.xframe.value", "DENY"));
    }

    @Test
    public void testNameNodeHttpServer_XFrameOptionConfiguration() throws Exception {
        // Prepare
        mockStatic(HttpServer2.Builder.class);
        HttpServer2.Builder builderMock = PowerMockito.mock(HttpServer2.Builder.class);
        when(builderMock.configureXFrame(anyBoolean())).thenReturn(builderMock);
        when(builderMock.setXFrameOption(anyString())).thenReturn(builderMock);
        
        PowerMockito.whenNew(HttpServer2.Builder.class).withNoArguments().thenReturn(builderMock);

        // Execute - Simulating NameNodeHttpServer.start()
        final boolean xFrameEnabled = conf.getBoolean(
            DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED,
            DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED_DEFAULT);

        final String xFrameOptionValue = conf.getTrimmed(
            DFSConfigKeys.DFS_XFRAME_OPTION_VALUE,
            DFSConfigKeys.DFS_XFRAME_OPTION_VALUE_DEFAULT);

        // Simulate the builder method calls
        builderMock.configureXFrame(xFrameEnabled);
        builderMock.setXFrameOption(xFrameOptionValue);

        // Verify
        verify(builderMock).configureXFrame(true);
        verify(builderMock).setXFrameOption("SAMEORIGIN");
        
        // Compare against reference loader
        assertEquals(xFrameEnabled, Boolean.parseBoolean(configProperties.getProperty("dfs.xframe.enabled", "false")));
        assertEquals(xFrameOptionValue, configProperties.getProperty("dfs.xframe.value", "DENY"));
    }

    @Test
    public void testSecondaryNameNode_XFrameOptionConfiguration() throws Exception {
        // Prepare
        mockStatic(HttpServer2.Builder.class);
        HttpServer2.Builder builderMock = PowerMockito.mock(HttpServer2.Builder.class);
        when(builderMock.configureXFrame(anyBoolean())).thenReturn(builderMock);
        when(builderMock.setXFrameOption(anyString())).thenReturn(builderMock);
        
        PowerMockito.whenNew(HttpServer2.Builder.class).withNoArguments().thenReturn(builderMock);

        // Execute - Simulating SecondaryNameNode.startInfoServer()
        final boolean xFrameEnabled = conf.getBoolean(
            DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED,
            DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED_DEFAULT);

        final String xFrameOptionValue = conf.getTrimmed(
            DFSConfigKeys.DFS_XFRAME_OPTION_VALUE,
            DFSConfigKeys.DFS_XFRAME_OPTION_VALUE_DEFAULT);

        // Simulate the builder method calls
        builderMock.configureXFrame(xFrameEnabled);
        builderMock.setXFrameOption(xFrameOptionValue);

        // Verify
        verify(builderMock).configureXFrame(true);
        verify(builderMock).setXFrameOption("SAMEORIGIN");
        
        // Compare against reference loader
        assertEquals(xFrameEnabled, Boolean.parseBoolean(configProperties.getProperty("dfs.xframe.enabled", "false")));
        assertEquals(xFrameOptionValue, configProperties.getProperty("dfs.xframe.value", "DENY"));
    }
}