package org.apache.hadoop.net;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import java.io.File;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestTableMappingConfiguration {

    // Test resolve() when topology file is not configured or is missing
    @Test
    public void testResolveWithMissingTopologyFile() {
        // Create a configuration instance and remove the topology file property
        Configuration conf = new Configuration();
        conf.unset(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY);

        // Create TableMapping instance and associate it with the configuration
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        // Prepare the list of node names
        List<String> nodeNames = Arrays.asList("node1.example.com", "node2.example.com", "node3.example.com");
        
        // Resolve the topology
        List<String> resolvedRackMappings = tableMapping.resolve(nodeNames);

        // Validate that all nodes map to the default rack
        for (String rack : resolvedRackMappings) {
            assertEquals(NetworkTopology.DEFAULT_RACK, rack);
        }
    }

    // Test resolve() with a valid topology file configured
    @Test
    public void testResolveWithValidTopologyFile() throws Exception {
        // Create a temporary valid topology file
        File tempFile = File.createTempFile("topology", "txt");
        tempFile.deleteOnExit();
        try (PrintWriter writer = new PrintWriter(tempFile)) {
            writer.println("node1.example.com /rackA");
            writer.println("node2.example.com /rackB");
        }

        // Configure the valid topology file path
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, tempFile.getAbsolutePath());

        // Create TableMapping instance and associate it with the configuration
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        // Prepare the list of node names
        List<String> nodeNames = Arrays.asList("node1.example.com", "node2.example.com", "node3.example.com");

        // Resolve the topology
        List<String> resolvedRackMappings = tableMapping.resolve(nodeNames);

        // Validate the mappings
        assertEquals("/rackA", resolvedRackMappings.get(0));
        assertEquals("/rackB", resolvedRackMappings.get(1));
        assertEquals(NetworkTopology.DEFAULT_RACK, resolvedRackMappings.get(2));
    }

    // Test resolving nodes after reloading cached mappings
    @Test
    public void testReloadingCachedMappings() throws Exception {
        // Create a temporary topology file
        File tempFile = File.createTempFile("initial_topology", "txt");
        tempFile.deleteOnExit();
        try (PrintWriter writer = new PrintWriter(tempFile)) {
            writer.println("node1.example.com /rackA");
        }

        // Configure the topology file path
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, tempFile.getAbsolutePath());

        // Create TableMapping instance and associate it with the configuration
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        // Validate the initial mapping
        assertEquals("/rackA", tableMapping.resolve(Arrays.asList("node1.example.com")).get(0));

        // Update the topology file
        try (PrintWriter writer = new PrintWriter(tempFile)) {
            writer.println("node1.example.com /rackUpdated");
        }

        // Reload mappings and validate the updated mappings
        tableMapping.reloadCachedMappings();
        assertEquals("/rackUpdated", tableMapping.resolve(Arrays.asList("node1.example.com")).get(0));
    }

    // Test resolve() with an invalid topology file format
    @Test
    public void testResolveWithInvalidTopologyFile() throws Exception {
        // Create a temporary invalid topology file
        File tempFile = File.createTempFile("invalid_topology", "txt");
        tempFile.deleteOnExit();
        try (PrintWriter writer = new PrintWriter(tempFile)) {
            writer.println("node1.example.com");
        }

        // Configure the invalid topology file path
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, tempFile.getAbsolutePath());

        // Create TableMapping instance and associate it with the configuration
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        // Prepare the list of node names
        List<String> nodeNames = Arrays.asList("node1.example.com", "node2.example.com");

        // Resolve the topology
        List<String> resolvedRackMappings = tableMapping.resolve(nodeNames);

        // Validate that all nodes map to the default rack due to invalid file format
        for (String rack : resolvedRackMappings) {
            assertEquals(NetworkTopology.DEFAULT_RACK, rack);
        }
    }
}