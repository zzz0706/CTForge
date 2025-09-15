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

    // Test case: Verify default behavior of resolve() when topology file is missing
    @Test
    public void testResolveWithDefaultRackWhenTopologyFileIsMissing() {
        Configuration conf = new Configuration();
        conf.unset(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY);

        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        List<String> nodeNames = Arrays.asList("node1.example.com", "node2.example.com", "node3.example.com");
        List<String> resolvedRackMappings = tableMapping.resolve(nodeNames);

        for (String rack : resolvedRackMappings) {
            assertEquals(NetworkTopology.DEFAULT_RACK, rack);
        }
    }

    // Test case: Verify resolve() behavior with a valid topology file
    @Test
    public void testResolveWithValidTopologyFile() throws Exception {
        File tempFile = File.createTempFile("topology", "txt");
        tempFile.deleteOnExit();
        try (PrintWriter writer = new PrintWriter(tempFile)) {
            writer.println("node1.example.com /rackA");
            writer.println("node2.example.com /rackB");
        }

        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, tempFile.getAbsolutePath());

        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        List<String> nodeNames = Arrays.asList("node1.example.com", "node2.example.com", "node3.example.com");
        List<String> resolvedRackMappings = tableMapping.resolve(nodeNames);

        assertEquals("/rackA", resolvedRackMappings.get(0));
        assertEquals("/rackB", resolvedRackMappings.get(1));
        assertEquals(NetworkTopology.DEFAULT_RACK, resolvedRackMappings.get(2));
    }

    // Test case: Verify reloadCachedMappings() with valid topology updates
    @Test
    public void testReloadingCachedMappingsWithValidTopologyFile() throws Exception {
        File tempFile = File.createTempFile("initial_topology", "txt");
        tempFile.deleteOnExit();
        try (PrintWriter writer = new PrintWriter(tempFile)) {
            writer.println("node1.example.com /rackA");
        }

        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, tempFile.getAbsolutePath());

        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);
        assertEquals("/rackA", tableMapping.resolve(Arrays.asList("node1.example.com")).get(0));

        try (PrintWriter writer = new PrintWriter(tempFile)) {
            writer.println("node1.example.com /rackUpdated");
        }

        tableMapping.reloadCachedMappings();
        assertEquals("/rackUpdated", tableMapping.resolve(Arrays.asList("node1.example.com")).get(0));
    }

    // Test case: Verify resolve() behavior with an invalid topology file
    @Test
    public void testHandlingInvalidTopologyFile() throws Exception {
        File tempFile = File.createTempFile("invalid_topology", "txt");
        tempFile.deleteOnExit();
        try (PrintWriter writer = new PrintWriter(tempFile)) {
            writer.println("node1.example.com");
        }

        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, tempFile.getAbsolutePath());
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        List<String> nodeNames = Arrays.asList("node1.example.com", "node2.example.com");
        List<String> resolvedRackMappings = tableMapping.resolve(nodeNames);

        for (String rack : resolvedRackMappings) {
            assertEquals(NetworkTopology.DEFAULT_RACK, rack);
        }
    }
}