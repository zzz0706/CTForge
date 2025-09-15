package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.*;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.junit.Assert.*;

public class DfsDataNodeTransferSocketRecvBufferSizeTest {

    private Configuration conf;
    private File tempConfigFile;

    @Before
    public void setUp() throws Exception {
        // Create temporary configuration file in XML format
        tempConfigFile = File.createTempFile("hdfs-test", ".xml");
        tempConfigFile.deleteOnExit();

        // Create XML configuration content
        String xmlContent = "<?xml version=\"1.0\"?>\n" +
                "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n" +
                "<configuration>\n" +
                "  <property>\n" +
                "    <name>" + DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY + "</name>\n" +
                "    <value>65536</value>\n" +
                "  </property>\n" +
                "</configuration>";

        try (FileWriter writer = new FileWriter(tempConfigFile)) {
            writer.write(xmlContent);
        }

        // Load configuration from file
        conf = new Configuration(false);
        conf.addResource(tempConfigFile.toURI().toURL());
    }

    @After
    public void tearDown() {
        if (tempConfigFile != null && tempConfigFile.exists()) {
            tempConfigFile.delete();
        }
    }

    @Test
    public void testTransferSocketRecvBufferSizeConfigLoadedCorrectly() {
        // Given: Configuration loaded from external file

        // When: DNConf is created with the configuration
        DNConf dnConf = new DNConf(conf);

        // Then: The configuration value should match what's in the file
        int expectedValue = 65536;
        assertEquals("Transfer socket receive buffer size should match config file value",
                expectedValue, dnConf.getTransferSocketRecvBufferSize());
    }

    @Test
    public void testDefaultTransferSocketRecvBufferSize() throws IOException {
        // Given: A configuration without the specific property set
        Configuration defaultConf = new Configuration(false);
        // Ensure no value is set for our key
        defaultConf.unset(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY);

        // When: DNConf is created
        DNConf dnConf = new DNConf(defaultConf);

        // Then: Should use default value (0)
        assertEquals("Should use default value when not configured",
                DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_DEFAULT,
                dnConf.getTransferSocketRecvBufferSize());
    }

    @Test
    public void testDataNodeStartsWithConfiguredBufferSize() throws Exception {
        // Given: Fully configured DataNode setup
        Configuration testConf = new Configuration(false);
        testConf.setInt(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY, 131072);

        // When & Then: Configuration is properly read
        try {
            DNConf dnConf = new DNConf(testConf);
            assertEquals(131072, dnConf.getTransferSocketRecvBufferSize());
        } catch (Exception e) {
            // Configuration loading itself should not fail due to buffer size setting
            fail("DNConf should be created with configured buffer size: " + e.getMessage());
        }
    }

    @Test
    public void testConfigurationValueMatchesExternalFile() {
        // Given: External configuration file and ConfigService (Configuration object)
        String key = DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY;
        
        // When: Getting value through Configuration API
        int configValue = conf.getInt(key, DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_DEFAULT);
        
        // Then: Should match the value set in XML
        assertEquals("Configuration service value should match file content",
                65536, configValue);
    }
    
    @Test
    public void testNegativeBufferSizeAllowedInConfig() {
        // Given: Configuration with negative buffer size
        Configuration testConf = new Configuration(false);
        testConf.setInt(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY, -1);

        // When: DNConf is created
        DNConf dnConf = new DNConf(testConf);

        // Then: Configuration value is stored as-is (validation may happen elsewhere)
        assertEquals("Negative value should be stored in configuration",
                -1, dnConf.getTransferSocketRecvBufferSize());
    }
    
    @Test
    public void testZeroBufferSizeAllowedInConfig() {
        // Given: Configuration with zero buffer size
        Configuration testConf = new Configuration(false);
        testConf.setInt(DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY, 0);

        // When: DNConf is created
        DNConf dnConf = new DNConf(testConf);

        // Then: Configuration value is stored correctly
        assertEquals("Zero value should be stored in configuration",
                0, dnConf.getTransferSocketRecvBufferSize());
    }
}