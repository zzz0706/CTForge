package org.apache.hadoop.net;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class TableMappingConfigTest {

    private File topologyFile;

    @Before
    public void setUp() throws IOException {
        // Create a temporary topology table file
        topologyFile = File.createTempFile("topology", ".txt");
        topologyFile.deleteOnExit();

        // Populate the file with test mappings
        try (FileWriter writer = new FileWriter(topologyFile)) {
            writer.write("# Topology file for test\n");
            writer.write("host1 /rack1\n");
            writer.write("host2.example.com /rack2\n");
            writer.write("192.168.1.1 /rack3\n");
        }
    }

    @After
    public void tearDown() {
        if (topologyFile != null) {
            topologyFile.delete();
        }
    }

    @Test
    public void testDefaultRackFallbackWhenFileNameNotSet() {
        // 1. Instantiate Configuration without setting net.topology.table.file.name
        Configuration conf = new Configuration();

        // 2. Build TableMapping instance and inject the configuration
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        // 3. Call resolve(List<String>) with arbitrary host names
        List<String> hosts = Arrays.asList("host1", "host2.example.com", "192.168.1.1");
        List<String> actualRacks = tableMapping.resolve(hosts);

        // 4. Compute expected rack (NetworkTopology.DEFAULT_RACK)
        String expectedRack = NetworkTopology.DEFAULT_RACK;

        // 5. Assert all returned racks equal the default rack
        assertEquals(hosts.size(), actualRacks.size());
        for (String actual : actualRacks) {
            assertEquals(expectedRack, actual);
        }
    }

    @Test
    public void testResolveWithValidTopologyFile() {
        // 1. Create configuration and set the topology file path
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, topologyFile.getAbsolutePath());

        // 2. Build TableMapping instance and inject the configuration
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        // 3. Call resolve with hosts that have mappings
        List<String> hosts = Arrays.asList("host1", "host2.example.com", "192.168.1.1");
        List<String> actualRacks = tableMapping.resolve(hosts);

        // 4. Verify the correct racks are returned
        assertEquals(3, actualRacks.size());
        assertEquals("/rack1", actualRacks.get(0));
        assertEquals("/rack2", actualRacks.get(1));
        assertEquals("/rack3", actualRacks.get(2));
    }

    @Test
    public void testResolveWithNonExistentFile() {
        // 1. Create configuration with non-existent file path
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, "/non/existent/file.txt");

        // 2. Build TableMapping instance and inject the configuration
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        // 3. Call resolve with arbitrary hosts
        List<String> hosts = Arrays.asList("host1", "host2");
        List<String> actualRacks = tableMapping.resolve(hosts);

        // 4. Verify default rack is used for all hosts
        assertEquals(2, actualRacks.size());
        for (String rack : actualRacks) {
            assertEquals(NetworkTopology.DEFAULT_RACK, rack);
        }
    }

    @Test
    public void testReloadCachedMappings() {
        // 1. Create configuration with topology file
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, topologyFile.getAbsolutePath());

        // 2. Build TableMapping instance and inject the configuration
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        // 3. Resolve hosts to populate cache
        List<String> hosts = Arrays.asList("host1");
        tableMapping.resolve(hosts);

        // 4. Modify the topology file
        try (FileWriter writer = new FileWriter(topologyFile, true)) {
            writer.write("newhost /newrack\n");
        } catch (IOException e) {
            fail("Failed to modify topology file");
        }

        // 5. Reload mappings
        tableMapping.reloadCachedMappings();

        // 6. Resolve the new host
        List<String> newHosts = Arrays.asList("newhost");
        List<String> actualRacks = tableMapping.resolve(newHosts);

        // 7. Verify new mapping is used
        assertEquals(1, actualRacks.size());
        assertEquals("/newrack", actualRacks.get(0));
    }
}