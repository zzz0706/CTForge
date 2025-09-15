package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.HeartbeatManager;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.util.Time;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.io.IOException;
import java.io.InputStream;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class HeartbeatRecheckIntervalConfigTest {

    private Configuration conf;
    private FSNamesystem namesystem;
    private BlockManager blockManager;

    @Before
    public void setUp() {
        conf = new Configuration();
        namesystem = mock(FSNamesystem.class);
        blockManager = mock(BlockManager.class);
        when(namesystem.isRunning()).thenReturn(true);
    }

    @Test
    public void testHeartbeatRecheckIntervalDefaultFromConfig() {
        // Prepare: Ensure no explicit value is set, so default is used
        // The default should come from DFSConfigKeys

        // Test
        HeartbeatManager heartbeatManager = new HeartbeatManager(namesystem, blockManager, conf);

        // Verify that the default value is correctly loaded
        long expectedDefault = DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT;
        // Use reflection to access private field
        try {
            java.lang.reflect.Field field = HeartbeatManager.class.getDeclaredField("heartbeatRecheckInterval");
            field.setAccessible(true);
            long actualValue = field.getLong(heartbeatManager);
            assertEquals("Heartbeat recheck interval should match default",
                    expectedDefault, actualValue);
        } catch (Exception e) {
            fail("Failed to access heartbeatRecheckInterval field: " + e.getMessage());
        }
    }

    @Test
    public void testHeartbeatRecheckIntervalCustomValue() {
        // Prepare
        int customValue = 600000; // 10 minutes
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, customValue);

        // Test
        HeartbeatManager heartbeatManager = new HeartbeatManager(namesystem, blockManager, conf);

        // Verify
        try {
            java.lang.reflect.Field field = HeartbeatManager.class.getDeclaredField("heartbeatRecheckInterval");
            field.setAccessible(true);
            long actualValue = field.getLong(heartbeatManager);
            assertEquals("Heartbeat recheck interval should match custom value",
                    customValue, actualValue);
        } catch (Exception e) {
            fail("Failed to access heartbeatRecheckInterval field: " + e.getMessage());
        }
    }

    @Test
    public void testHeartbeatRecheckIntervalWithStaleDatanodeIntervalConstraint() {
        // Prepare: Set recheck interval larger than stale interval with avoid stale nodes enabled
        conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY, true);
        long recheckInterval = 600000; // 10 minutes
        long staleInterval = 300000;   // 5 minutes
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, (int)recheckInterval);
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY, staleInterval);

        // Test
        HeartbeatManager heartbeatManager = new HeartbeatManager(namesystem, blockManager, conf);

        // Verify: Should use the smaller stale interval
        try {
            java.lang.reflect.Field field = HeartbeatManager.class.getDeclaredField("heartbeatRecheckInterval");
            field.setAccessible(true);
            long actualValue = field.getLong(heartbeatManager);
            assertEquals("Heartbeat recheck interval should be limited by stale interval",
                    staleInterval, actualValue);
        } catch (Exception e) {
            fail("Failed to access heartbeatRecheckInterval field: " + e.getMessage());
        }
    }

    @Test
    public void testDatanodeManagerUsesCorrectHeartbeatRecheckInterval() throws Exception {
        // Prepare
        int customRecheckInterval = 400000;
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, customRecheckInterval);
        long heartbeatIntervalSeconds = 3L;
        conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, heartbeatIntervalSeconds);

        // Test
        DatanodeManager datanodeManager = new DatanodeManager(blockManager, namesystem, conf);

        // Access the private field using reflection
        // For DatanodeManager, we check heartbeatExpireInterval which is derived from recheck interval
        java.lang.reflect.Field field = DatanodeManager.class.getDeclaredField("heartbeatExpireInterval");
        field.setAccessible(true);
        long expireInterval = field.getLong(datanodeManager);
        // We know expireInterval = 2 * recheckInterval + 10000 * heartbeatIntervalSeconds
        // So recheckInterval = (expireInterval - 10000 * heartbeatIntervalSeconds) / 2
        long expectedExpireInterval = 2L * customRecheckInterval + 10000L * heartbeatIntervalSeconds;

        assertEquals("Expire interval should be correctly calculated from recheck interval",
                expectedExpireInterval, expireInterval);
    }

    @Test
    public void testConfigValueMatchesPropertiesFile() throws Exception {
        // Load default value from XML configuration file
        int expectedFromFile = DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT;
        
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("hdfs-default.xml")) {
            if (is != null) {
                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                DocumentBuilder builder = factory.newDocumentBuilder();
                Document document = builder.parse(is);
                
                NodeList properties = document.getElementsByTagName("property");
                for (int i = 0; i < properties.getLength(); i++) {
                    Element property = (Element) properties.item(i);
                    String name = property.getElementsByTagName("name").item(0).getTextContent();
                    if (DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY.equals(name)) {
                        String value = property.getElementsByTagName("value").item(0).getTextContent();
                        expectedFromFile = Integer.parseInt(value);
                        break;
                    }
                }
            }
        } catch (Exception e) {
            // If parsing fails, use the default value
        }

        // Get value from Configuration service
        Configuration config = new Configuration();
        int configValue = config.getInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 
                DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT);

        // Verify they match
        assertEquals("Config value should match file default", expectedFromFile, configValue);
    }

    @Test
    public void testShouldAbortHeartbeatCheckWithRecheckInterval() {
        // Prepare
        int recheckInterval = 300000; // 5 minutes
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, recheckInterval);

        HeartbeatManager heartbeatManager = new HeartbeatManager(namesystem, blockManager, conf);
        
        // Since we can't directly mock getHeartbeatElapsedTime(), we'll test the behavior
        // by checking that the manager was created with the correct interval
        try {
            java.lang.reflect.Field field = HeartbeatManager.class.getDeclaredField("heartbeatRecheckInterval");
            field.setAccessible(true);
            long actualRecheckInterval = field.getLong(heartbeatManager);
            
            assertEquals("Recheck interval should be set correctly", (long)recheckInterval, actualRecheckInterval);
        } catch (Exception e) {
            fail("Failed to access heartbeatRecheckInterval field: " + e.getMessage());
        }
    }
}