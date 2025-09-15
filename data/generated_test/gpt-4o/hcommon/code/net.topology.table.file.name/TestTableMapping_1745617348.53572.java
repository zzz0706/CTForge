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

    // Test the resolve() method with a valid topology file
    @Test
    public void testResolveWithValidTopologyFile() throws Exception {
        // Create a temporary topology file
        File tempTopologyFile = File.createTempFile("valid_topology", ".txt");
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

        // Prepare the input conditions for unit testing
        List<String> inputNodeNames = Arrays.asList("node1", "node2");
        List<String> rackMappings = tableMapping.resolve(inputNodeNames);

        // Verify the mappings returned from the topology file
        assertNotNull(rackMappings);
        assertEquals(2, rackMappings.size());
        assertEquals("/rack1", rackMappings.get(0));
        assertEquals("/rack2", rackMappings.get(1));
    }

    // Test the behavior of the resolve() method when no topology file is configured
    @Test
    public void testResolveWithoutConfiguration() {
        // Initialize configuration without setting topology file
        Configuration conf = new Configuration();
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        // Prepare input conditions for unit testing
        List<String> inputNodeNames = Arrays.asList("node1", "node2");
        List<String> rackMappings = tableMapping.resolve(inputNodeNames);

        // Verify fallback to default rack mappings
        assertNotNull(rackMappings);
        assertEquals(inputNodeNames.size(), rackMappings.size());
        for (String rack : rackMappings) {
            assertEquals(NetworkTopology.DEFAULT_RACK, rack);
        }
    }

    // Test the reloadCachedMappings() method with an updated topology file
    @Test
    public void testReloadCachedMappings() throws Exception {
        // Create and populate the initial topology file
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

        // Verify initial mappings
        List<String> initialNodeNames = Arrays.asList("node1", "node2");
        List<String> initialMappings = tableMapping.resolve(initialNodeNames);
        assertNotNull(initialMappings);
        assertEquals("/rack1", initialMappings.get(0));
        assertEquals(NetworkTopology.DEFAULT_RACK, initialMappings.get(1));

        // Update topology file with new entries
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(initialTopologyFile))) {
            writer.write("node1 /rack1\n");
            writer.write("node2 /rack2\n");
        }

        // Reload and verify the updated mappings
        tableMapping.reloadCachedMappings();
        List<String> updatedMappings = tableMapping.resolve(initialNodeNames);
        assertNotNull(updatedMappings);
        assertEquals("/rack1", updatedMappings.get(0));
        assertEquals("/rack2", updatedMappings.get(1));
    }

    // Test the resolve() method with partially missing mappings in the topology file
    @Test
    public void testResolveWithPartialMappings() throws Exception {
        // Create a topology file with one valid mapping
        File partialTopologyFile = File.createTempFile("partial_topology", ".txt");
        partialTopologyFile.deleteOnExit();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(partialTopologyFile))) {
            writer.write("node1 /rack1\n");
        }

        // Get configuration value using API
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, partialTopologyFile.getAbsolutePath());
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        // Prepare and resolve node names
        List<String> inputNodeNames = Arrays.asList("node1", "node2", "node3");
        List<String> rackMappings = tableMapping.resolve(inputNodeNames);

        // Verify the results for partially resolved mappings
        assertNotNull(rackMappings);
        assertEquals("/rack1", rackMappings.get(0)); // Valid mapping
        assertEquals(NetworkTopology.DEFAULT_RACK, rackMappings.get(1)); // Missing mapping
        assertEquals(NetworkTopology.DEFAULT_RACK, rackMappings.get(2)); // Missing mapping
    }
}