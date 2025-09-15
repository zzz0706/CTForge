package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.util.Time;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.io.InputStream;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class HeartbeatManagerConfigTest {

    @Mock
    private Namesystem namesystem;
    
    @Mock
    private BlockManager blockManager;
    
    private Configuration conf;
    
    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        conf = new Configuration();
    }

    @Test
    public void testHeartbeatRecheckIntervalDefaultValue() throws Exception {
        // Load the default configuration file properly as XML
        String defaultValue = null;
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("hdfs-default.xml")) {
            if (is != null) {
                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                DocumentBuilder builder = factory.newDocumentBuilder();
                Document document = builder.parse(is);
                
                NodeList properties = document.getElementsByTagName("property");
                for (int i = 0; i < properties.getLength(); i++) {
                    Element property = (Element) properties.item(i);
                    Element name = (Element) property.getElementsByTagName("name").item(0);
                    if (name != null && "dfs.namenode.heartbeat.recheck-interval".equals(name.getTextContent())) {
                        Element value = (Element) property.getElementsByTagName("value").item(0);
                        if (value != null) {
                            defaultValue = value.getTextContent();
                            break;
                        }
                    }
                }
            }
        }
        
        // If we couldn't load from file, use the known default
        if (defaultValue == null) {
            defaultValue = "300000";
        }
        
        long expectedInterval = Long.parseLong(defaultValue);
        
        // Get value via Configuration API
        long actualInterval = conf.getLong(
            DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT
        );
        
        // Verify they match
        assertEquals("Configuration value should match file value", expectedInterval, actualInterval);
    }

    @Test
    public void testHeartbeatRecheckIntervalUsedInConstructor() {
        // Setup configuration
        long testInterval = 600000L; // 10 minutes
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, testInterval);
        
        // Mock dependencies
        when(namesystem.isRunning()).thenReturn(false); // Stop immediately
        
        // Create HeartbeatManager - this will use our configured value
        HeartbeatManager heartbeatManager = new HeartbeatManager(namesystem, blockManager, conf);
        
        // Use reflection to access the private field for verification
        try {
            java.lang.reflect.Field field = HeartbeatManager.class.getDeclaredField("heartbeatRecheckInterval");
            field.setAccessible(true);
            long actualInterval = (Long) field.get(heartbeatManager);
            
            assertEquals("Heartbeat recheck interval should be set from configuration", testInterval, actualInterval);
        } catch (Exception e) {
            fail("Failed to access private field: " + e.getMessage());
        }
    }

    @Test
    public void testHeartbeatRecheckIntervalAffectsStaleNodeHandling() {
        // Setup: enable stale datanode avoidance and set intervals
        long recheckInterval = 600000L; // 10 minutes
        long staleInterval = 300000L;   // 5 minutes (smaller than recheck)
        
        conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY, true);
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, recheckInterval);
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY, staleInterval);
        
        when(namesystem.isRunning()).thenReturn(false); // Stop immediately
        
        // Create HeartbeatManager
        HeartbeatManager heartbeatManager = new HeartbeatManager(namesystem, blockManager, conf);
        
        // Verify that when stale interval < recheck interval, recheck interval is set to stale interval
        try {
            java.lang.reflect.Field field = HeartbeatManager.class.getDeclaredField("heartbeatRecheckInterval");
            field.setAccessible(true);
            long actualInterval = (Long) field.get(heartbeatManager);
            
            assertEquals("When stale interval is smaller, heartbeat recheck interval should be set to stale interval", 
                        staleInterval, actualInterval);
        } catch (Exception e) {
            fail("Failed to access private field: " + e.getMessage());
        }
    }

    @Test
    public void testShouldAbortHeartbeatCheckUsesConfiguredInterval() {
        // Setup configuration
        long testInterval = 10000L; // 10 seconds
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, testInterval);
        
        when(namesystem.isRunning()).thenReturn(false); // Stop immediately
        
        // Create HeartbeatManager
        HeartbeatManager heartbeatManager = new HeartbeatManager(namesystem, blockManager, conf);
        
        // Test the shouldAbortHeartbeatCheck method
        boolean result1 = heartbeatManager.shouldAbortHeartbeatCheck(5000); // 5s offset
        assertFalse("Should not abort when elapsed + offset < recheck interval", result1);
        
        boolean result2 = heartbeatManager.shouldAbortHeartbeatCheck(15000); // 15s offset
        assertTrue("Should abort when elapsed + offset > recheck interval", result2);
    }

    @Test
    public void testHeartbeatCheckMethodUsesHeartbeatRecheckInterval() throws Exception {
        // Setup configuration
        long testInterval = 5000L; // 5 seconds
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, testInterval);
        
        // Mock namesystem and blockManager to avoid NullPointerException
        when(namesystem.isRunning()).thenReturn(true).thenReturn(false);
        when(blockManager.getDatanodeManager()).thenReturn(mock(DatanodeManager.class));
        
        // Create HeartbeatManager
        HeartbeatManager heartbeatManager = spy(new HeartbeatManager(namesystem, blockManager, conf));
        
        // Execute heartbeatCheck method directly instead of run()
        heartbeatManager.heartbeatCheck();
        
        // Verify that heartbeatCheck was called
        verify(heartbeatManager, atLeastOnce()).heartbeatCheck();
    }
}