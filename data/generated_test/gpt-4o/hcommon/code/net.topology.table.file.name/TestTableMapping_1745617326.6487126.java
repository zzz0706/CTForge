package org.apache.hadoop.net;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.net.TableMapping;
import org.apache.hadoop.net.NetworkTopology;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class TestTableMapping {

    // Test configuration propagation and usage during load
    @Test
    public void testLoadWithValidConfiguration() throws Exception {
        // Create a temporary topology file
        File tempTopologyFile = File.createTempFile("topology", ".txt");
        tempTopologyFile.deleteOnExit();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempTopologyFile))) {
            writer.write("node1 /rack1\n");
            writer.write("node2 /rack2\n");
        }

        // Get configuration value using API
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, tempTopologyFile.getAbsolutePath());

        // Initialize TableMapping with the configuration
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        // Test internal load method indirectly via resolve()
        List<String> inputNodeNames = Arrays.asList("node1", "node2", "node3");
        List<String> rackMappings = tableMapping.resolve(inputNodeNames);

        // Verify results from the loaded topology
        assertNotNull(rackMappings);
        assertEquals("/rack1", rackMappings.get(0));
        assertEquals("/rack2", rackMappings.get(1));
        assertEquals(NetworkTopology.DEFAULT_RACK, rackMappings.get(2));
    }

    // Test resolve method with missing configuration
    @Test
    public void testResolveWithoutTopologyFile() {
        // Initialize configuration without setting topology file
        Configuration conf = new Configuration();
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        // Prepare input conditions for unit testing
        List<String> inputNodeNames = Arrays.asList("node1", "node2");
        List<String> rackMappings = tableMapping.resolve(inputNodeNames);

        // Verify fallback to default rack when configuration is missing
        assertNotNull(rackMappings);
        assertEquals(inputNodeNames.size(), rackMappings.size());
        for (String rack : rackMappings) {
            assertEquals(NetworkTopology.DEFAULT_RACK, rack);
        }
    }

    // Test reloadCachedMappings functionality
    @Test
    public void testReloadCachedMappingsWithUpdatedFile() throws Exception {
        // Create and write initial topology file
        File initialTopologyFile = File.createTempFile("initial_topology", ".txt");
        initialTopologyFile.deleteOnExit();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(initialTopologyFile))) {
            writer.write("node1 /rack1\n");
        }

        // Get configuration value using API
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, initialTopologyFile.getAbsolutePath());
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        // Verify resolution against initial topology
        List<String> inputNodeNames = Arrays.asList("node1", "node2");
        List<String> initialMappings = tableMapping.resolve(inputNodeNames);
        assertNotNull(initialMappings);
        assertEquals("/rack1", initialMappings.get(0));
        assertEquals(NetworkTopology.DEFAULT_RACK, initialMappings.get(1));

        // Update topology file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(initialTopologyFile))) {
            writer.write("node1 /rack1\n");
            writer.write("node2 /rack2\n");
        }

        // Reload cached mappings and verify updated resolution
        tableMapping.reloadCachedMappings();
        List<String> updatedMappings = tableMapping.resolve(inputNodeNames);
        assertNotNull(updatedMappings);
        assertEquals("/rack1", updatedMappings.get(0));
        assertEquals("/rack2", updatedMappings.get(1));
    }

    // Test resolve method behavior with partially invalid input
    @Test
    public void testResolveWithPartialInvalidMapping() throws Exception {
        // Create a topology file with partial mappings
        File topologyFile = File.createTempFile("partial_topology", ".txt");
        topologyFile.deleteOnExit();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(topologyFile))) {
            writer.write("node1 /rack1\n");
        }

        // Get configuration value using API
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, topologyFile.getAbsolutePath());
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        // Prepare input and test resolution with mixed valid/invalid mappings
        List<String> inputNodeNames = Arrays.asList("node1", "node2", "node3");
        List<String> rackMappings = tableMapping.resolve(inputNodeNames);
        assertNotNull(rackMappings);
        assertEquals("/rack1", rackMappings.get(0)); // Valid mapping
        assertEquals(NetworkTopology.DEFAULT_RACK, rackMappings.get(1)); // Missing mapping
        assertEquals(NetworkTopology.DEFAULT_RACK, rackMappings.get(2)); // Missing mapping
    }
}