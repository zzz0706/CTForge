package org.apache.hadoop.net;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.TableMapping;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class TestTableMapping {
    // Test resolve() with a valid topology file
    @Test
    public void testResolveWithValidTopologyFile() throws Exception {
        // Create a temporary topology file
        File tempTopologyFile = File.createTempFile("topology", ".txt");
        tempTopologyFile.deleteOnExit();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempTopologyFile))) {
            writer.write("node1 /rack1\n");
            writer.write("node2 /rack2\n");
            writer.write("node3 /rack3\n");
        }

        // Set up Hadoop configuration
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, tempTopologyFile.getAbsolutePath());

        // Initialize TableMapping with the configuration
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        // Prepare input nodes and call resolve()
        List<String> inputNodeNames = Arrays.asList("node1", "node2", "node3", "node4");
        List<String> rackMappings = tableMapping.resolve(inputNodeNames);

        // Verify results
        assertNotNull(rackMappings);
        assertEquals(inputNodeNames.size(), rackMappings.size());
        assertEquals("/rack1", rackMappings.get(0));
        assertEquals("/rack2", rackMappings.get(1));
        assertEquals("/rack3", rackMappings.get(2));
        assertEquals(NetworkTopology.DEFAULT_RACK, rackMappings.get(3));
    }

    // Test reloadCachedMappings()
    @Test
    public void testReloadCachedMappings() throws Exception {
        // Create a temporary initial topology file
        File initialTopologyFile = File.createTempFile("initial_topology", ".txt");
        initialTopologyFile.deleteOnExit();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(initialTopologyFile))) {
            writer.write("node1 /rack1\n");
        }

        // Set up Hadoop configuration
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, initialTopologyFile.getAbsolutePath());
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        // Verify initial mapping
        List<String> initialMapping = tableMapping.resolve(Arrays.asList("node1", "node2"));
        assertNotNull(initialMapping);
        assertEquals("/rack1", initialMapping.get(0));
        assertEquals(NetworkTopology.DEFAULT_RACK, initialMapping.get(1));

        // Create a new topology file
        File updatedTopologyFile = new File(initialTopologyFile.getAbsolutePath());
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(updatedTopologyFile))) {
            writer.write("node1 /rack1\n");
            writer.write("node2 /rack2\n");
        }

        // Reload mappings and verify
        tableMapping.reloadCachedMappings();
        List<String> updatedMapping = tableMapping.resolve(Arrays.asList("node1", "node2"));
        assertNotNull(updatedMapping);
        assertEquals("/rack1", updatedMapping.get(0));
        assertEquals("/rack2", updatedMapping.get(1));
    }

    // Test resolve() with missing configuration
    @Test
    public void testResolveWithoutConfiguration() {
        // Initialize TableMapping without providing a topology file configuration
        TableMapping tableMapping = new TableMapping();
        Configuration conf = new Configuration();
        tableMapping.setConf(conf);

        List<String> inputNodeNames = Arrays.asList("node1", "node2");
        List<String> rackMappings = tableMapping.resolve(inputNodeNames);

        // Verify that all nodes resolve to the default rack
        assertNotNull(rackMappings);
        assertEquals(inputNodeNames.size(), rackMappings.size());
        for (String rack : rackMappings) {
            assertEquals(NetworkTopology.DEFAULT_RACK, rack);
        }
    }
}