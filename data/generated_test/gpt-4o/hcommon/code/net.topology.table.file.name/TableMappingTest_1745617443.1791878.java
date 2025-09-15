package org.apache.hadoop.net;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;
import java.io.File;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TableMappingTest {

    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void testResolveWithDefaultRackWhenTopologyFileIsMissing() {
        // Create a Configuration object with no topology file configured
        Configuration conf = new Configuration();
        conf.unset(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY);

        // Ensure the topology file configuration is unset or invalid
        String topologyFileName = conf.get(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, null);
        assertEquals(null, topologyFileName);

        // Initialize TableMapping and test resolve method
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        List<String> nodeNames = Arrays.asList("node1.example.com", "node2.example.com", "node3.example.com");
        List<String> resolvedRackMappings = tableMapping.resolve(nodeNames);

        // Verify all nodes are mapped to '/default-rack' when topology file is not configured or missing
        for (String rack : resolvedRackMappings) {
            assertEquals(NetworkTopology.DEFAULT_RACK, rack);
        }
    }

    @Test
    public void testResolveWithValidTopologyFile() throws Exception {
        // Create a temporary topology file with valid mappings
        File tempFile = File.createTempFile("topology", "txt");
        tempFile.deleteOnExit();
        try (PrintWriter writer = new PrintWriter(tempFile)) {
            // Write valid topology entries: node1 -> /rackA, node2 -> /rackB
            writer.println("node1.example.com /rackA");
            writer.println("node2.example.com /rackB");
        }

        // Create and configure a Configuration object pointing to the topology file
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, tempFile.getAbsolutePath());

        // Verify the topology file path configuration
        String topologyFileName = conf.get(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, null);
        assertEquals(tempFile.getAbsolutePath(), topologyFileName);

        // Initialize TableMapping and test resolve method
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        List<String> nodeNames = Arrays.asList("node1.example.com", "node2.example.com", "node3.example.com");
        List<String> resolvedRackMappings = tableMapping.resolve(nodeNames);

        // Verify the rack mappings resolved based on the topology file
        assertEquals("/rackA", resolvedRackMappings.get(0));
        assertEquals("/rackB", resolvedRackMappings.get(1));
        assertEquals(NetworkTopology.DEFAULT_RACK, resolvedRackMappings.get(2));
    }

    @Test
    public void testReloadingCachedMappingsWithValidTopologyFile() throws Exception {
        // Step 1: Create a temporary topology file with initial mappings
        File tempFile = File.createTempFile("initial_topology", "txt");
        tempFile.deleteOnExit();
        try (PrintWriter writer = new PrintWriter(tempFile)) {
            writer.println("node1.example.com /rackA");
        }

        // Step 2: Configure the topology file path in Configuration
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, tempFile.getAbsolutePath());

        // Step 3: Initialize TableMapping and resolve
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);
        assertEquals("/rackA", tableMapping.resolve(Arrays.asList("node1.example.com")).get(0));

        // Step 4: Modify the topology file with updated mappings
        try (PrintWriter writer = new PrintWriter(tempFile)) {
            writer.println("node1.example.com /rackUpdated");
        }

        // Step 5: Reload cached mappings
        tableMapping.reloadCachedMappings();

        // Step 6: Verify the updated mappings
        assertEquals("/rackUpdated", tableMapping.resolve(Arrays.asList("node1.example.com")).get(0));
    }

    @Test
    public void testHandlingInvalidTopologyFile() throws Exception {
        // Create a temporary topology file with invalid entries
        File tempFile = File.createTempFile("invalid_topology", "txt");
        tempFile.deleteOnExit();
        try (PrintWriter writer = new PrintWriter(tempFile)) {
            // Write invalid topology entries (missing rack)
            writer.println("node1.example.com");
        }

        // Configure a Configuration object pointing to the invalid topology file
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, tempFile.getAbsolutePath());
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        // Attempt to resolve nodes; all nodes should default to '/default-rack'
        List<String> nodeNames = Arrays.asList("node1.example.com", "node2.example.com");
        List<String> resolvedRackMappings = tableMapping.resolve(nodeNames);

        for (String rack : resolvedRackMappings) {
            assertEquals(NetworkTopology.DEFAULT_RACK, rack);
        }
    }
}