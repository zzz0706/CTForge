package org.apache.hadoop.net;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import java.io.File;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestTableMappingConfiguration {

    // Test resolve() when topology file is not configured or is missing
    @Test
    public void testResolveWithMissingTopologyFile() {
        // Get configuration value using API
        Configuration conf = new Configuration();
        conf.unset(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY);

        // Prepare the input conditions for unit testing.
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        List<String> nodeNames = Arrays.asList("node1.example.com", "node2.example.com", "node3.example.com");
        List<String> resolvedRackMappings = tableMapping.resolve(nodeNames);

        // Test code
        for (String rack : resolvedRackMappings) {
            assertEquals(NetworkTopology.DEFAULT_RACK, rack);
        }
    }

    // Test resolve() with a valid topology file configured
    @Test
    public void testResolveWithValidTopologyFile() throws Exception {
        // Get configuration value using API
        File tempFile = File.createTempFile("topology", "txt");
        tempFile.deleteOnExit();
        try (PrintWriter writer = new PrintWriter(tempFile)) {
            writer.println("node1.example.com /rackA");
            writer.println("node2.example.com /rackB");
        }

        // Prepare the input conditions for unit testing.
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, tempFile.getAbsolutePath());

        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        List<String> nodeNames = Arrays.asList("node1.example.com", "node2.example.com", "node3.example.com");
        List<String> resolvedRackMappings = tableMapping.resolve(nodeNames);

        // Test code
        assertEquals("/rackA", resolvedRackMappings.get(0));
        assertEquals("/rackB", resolvedRackMappings.get(1));
        assertEquals(NetworkTopology.DEFAULT_RACK, resolvedRackMappings.get(2));
    }

    // Test reloading cached mappings and ensure resolve() uses updated mapping
    @Test
    public void testReloadingCachedMappings() throws Exception {
        // Get configuration value using API
        File tempFile = File.createTempFile("initial_topology", "txt");
        tempFile.deleteOnExit();
        try (PrintWriter writer = new PrintWriter(tempFile)) {
            writer.println("node1.example.com /rackA");
        }

        // Prepare the input conditions for unit testing.
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, tempFile.getAbsolutePath());

        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        // Test initial mapping
        assertEquals("/rackA", tableMapping.resolve(Arrays.asList("node1.example.com")).get(0));

        // Update topology file to emulate reloading cached mappings
        try (PrintWriter writer = new PrintWriter(tempFile)) {
            writer.println("node1.example.com /rackB");
        }

        // Reload and verify updated mapping
        tableMapping.reloadCachedMappings();
        assertEquals("/rackB", tableMapping.resolve(Arrays.asList("node1.example.com")).get(0));
    }

    // Test resolve() with an invalid topology file format
    @Test
    public void testResolveWithInvalidTopologyFile() throws Exception {
        // Get configuration value using API
        File tempFile = File.createTempFile("invalid_topology", "txt");
        tempFile.deleteOnExit();
        try (PrintWriter writer = new PrintWriter(tempFile)) {
            writer.println("node1.example.com");  // Invalid format (missing rack)
        }

        // Prepare the input conditions for unit testing.
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, tempFile.getAbsolutePath());

        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        List<String> nodeNames = Arrays.asList("node1.example.com", "node2.example.com");
        List<String> resolvedRackMappings = tableMapping.resolve(nodeNames);

        // Test code
        for (String rack : resolvedRackMappings) {
            assertEquals(NetworkTopology.DEFAULT_RACK, rack);
        }
    }

    // Test load() method indirectly via resolve() when configuration is set but file is empty
    @Test
    public void testLoadWithEmptyTopologyFile() throws Exception {
        // Get configuration value using API
        File tempFile = File.createTempFile("empty_topology", "txt");
        tempFile.deleteOnExit();

        // Prepare the input conditions for unit testing.
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, tempFile.getAbsolutePath());

        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        List<String> nodeNames = Arrays.asList("node1.example.com", "node2.example.com");
        List<String> resolvedRackMappings = tableMapping.resolve(nodeNames);

        // Test code
        for (String rack : resolvedRackMappings) {
            assertEquals(NetworkTopology.DEFAULT_RACK, rack);
        }
    }
}