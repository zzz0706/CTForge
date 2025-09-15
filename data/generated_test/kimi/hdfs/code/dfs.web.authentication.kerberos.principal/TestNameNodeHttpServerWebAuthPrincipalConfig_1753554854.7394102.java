package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest({UserGroupInformation.class, SecurityUtil.class, HttpServer2.class, WebHdfsFileSystem.class})
public class TestNameNodeHttpServerWebAuthPrincipalConfig {

    private Configuration conf;
    private InetSocketAddress bindAddress;

    @Before
    public void setUp() throws Exception {
        // Prepare the configuration
        conf = new Configuration();
        bindAddress = new InetSocketAddress("localhost", 9870);

        // Mock static methods
        mockStatic(UserGroupInformation.class);
        mockStatic(SecurityUtil.class);
        mockStatic(HttpServer2.class);
        mockStatic(WebHdfsFileSystem.class);
    }

    @Test
    // testWebAuthPrincipalNotSetLogsErrorWhenSecurityEnabled
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions: Security enabled, WebHDFS enabled, but dfs.web.authentication.kerberos.principal not set
    // 3. Test code: Create NameNodeHttpServer and call start() which will invoke initWebHdfs() and getAuthFilterParams()
    // 4. Code after testing: Verify that error log was called
    public void testWebAuthPrincipalNotSetLogsErrorWhenSecurityEnabled() throws Exception {
        // Arrange
        // Ensure security is enabled
        PowerMockito.when(UserGroupInformation.isSecurityEnabled()).thenReturn(true);
        
        // Ensure WebHDFS is enabled
        PowerMockito.when(WebHdfsFileSystem.isEnabled(conf)).thenReturn(true);
        
        // Ensure dfs.web.authentication.kerberos.principal is not set
        conf.unset(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY);
        
        // Mock HttpServer2.LOG to capture error logs
        org.apache.commons.logging.Log mockLog = mock(org.apache.commons.logging.Log.class);
        PowerMockito.field(HttpServer2.class, "LOG").set(HttpServer2.class, mockLog);
        
        // Create NameNodeHttpServer instance
        NameNodeHttpServer nameNodeHttpServer = new NameNodeHttpServer(conf, null, bindAddress);
        
        // Act
        // We need to mock some dependencies to allow the server to start
        HttpServer2 mockHttpServer = mock(HttpServer2.class);
        when(mockHttpServer.getWebAppContext()).thenReturn(mock(org.mortbay.jetty.webapp.WebAppContext.class));
        
        // Use reflection to set the private field
        java.lang.reflect.Field httpServerField = NameNodeHttpServer.class.getDeclaredField("httpServer");
        httpServerField.setAccessible(true);
        httpServerField.set(nameNodeHttpServer, mockHttpServer);
        
        // Call initWebHdfs which will invoke getAuthFilterParams
        java.lang.reflect.Method initWebHdfsMethod = NameNodeHttpServer.class.getDeclaredMethod("initWebHdfs", Configuration.class);
        initWebHdfsMethod.setAccessible(true);
        initWebHdfsMethod.invoke(nameNodeHttpServer, conf);
        
        // Verify that HttpServer2.LOG.error was called with expected message
        verify(mockLog).error(
            "WebHDFS and security are enabled, but configuration property '" +
            DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY +
            "' is not set."
        );
    }

    @Test
    // testWebAuthPrincipalIsUsedInAuthFilterParams
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions: Security enabled, WebHDFS enabled, and dfs.web.authentication.kerberos.principal is set
    // 3. Test code: Create NameNodeHttpServer and call start() which will invoke initWebHdfs() and getAuthFilterParams()
    // 4. Code after testing: Verify that the principal is correctly resolved and used
    public void testWebAuthPrincipalIsUsedInAuthFilterParams() throws Exception {
        // Arrange
        String principalValue = "HTTP/_HOST@EXAMPLE.COM";
        String expectedResolvedPrincipal = "HTTP/localhost@EXAMPLE.COM";
        
        // Set configuration value
        conf.set(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, principalValue);
        
        // Enable security
        PowerMockito.when(UserGroupInformation.isSecurityEnabled()).thenReturn(true);
        
        // Enable WebHDFS
        PowerMockito.when(WebHdfsFileSystem.isEnabled(conf)).thenReturn(true);
        
        // Mock SecurityUtil.getServerPrincipal to return our expected value
        PowerMockito.when(SecurityUtil.getServerPrincipal(principalValue, bindAddress.getHostName()))
                .thenReturn(expectedResolvedPrincipal);
        
        // Create NameNodeHttpServer instance
        NameNodeHttpServer nameNodeHttpServer = new NameNodeHttpServer(conf, null, bindAddress);
        
        // Mock HttpServer2.defineFilter to capture the params
        HttpServer2 mockHttpServer = mock(HttpServer2.class);
        when(mockHttpServer.getWebAppContext()).thenReturn(mock(org.mortbay.jetty.webapp.WebAppContext.class));
        
        // Use reflection to set the private field
        java.lang.reflect.Field httpServerField = NameNodeHttpServer.class.getDeclaredField("httpServer");
        httpServerField.setAccessible(true);
        httpServerField.set(nameNodeHttpServer, mockHttpServer);
        
        // Act
        java.lang.reflect.Method initWebHdfsMethod = NameNodeHttpServer.class.getDeclaredMethod("initWebHdfs", Configuration.class);
        initWebHdfsMethod.setAccessible(true);
        initWebHdfsMethod.invoke(nameNodeHttpServer, conf);
        
        // Verify that HttpServer2.defineFilter was called with correct params
        // First set up the verification context for static methods
        PowerMockito.verifyStatic(times(1));
        ArgumentCaptor<Map> paramsCaptor = ArgumentCaptor.forClass(Map.class);
        HttpServer2.defineFilter(
            any(org.mortbay.jetty.servlet.Context.class), 
            anyString(), 
            anyString(), 
            paramsCaptor.capture(), 
            (String[]) any()
        );
        
        Map<String, String> capturedParams = paramsCaptor.getValue();
        assertTrue("Params should contain the principal key", 
                  capturedParams.containsKey(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY));
        assertEquals("Resolved principal should match expected value", 
                    expectedResolvedPrincipal, 
                    capturedParams.get(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY));
    }
}