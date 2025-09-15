package org.apache.hadoop.net;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TableMappingTest {

    // Test the behavior when the configuration is blank
    @Test
    public void testLoadWithBlankConfiguration() {
        // Step 1: Create a Configuration object without setting the property
        Configuration conf = new Configuration();

        // Step 2: Initialize the TableMapping instance with the empty configuration
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        // Step 3: Verify load() behavior by forcing resolution to invoke it internally
        List<String> names = new ArrayList<>();
        names.add("host1.example.com");
        List<String> resolvedNames = tableMapping.resolve(names);

        // Step 4: Check that the map is null and fallback occurs
        assertEquals(1, resolvedNames.size());
        assertEquals(NetworkTopology.DEFAULT_RACK, resolvedNames.get(0));
    }

    // Test the behavior when an invalid file path is provided in the configuration
    @Test
    public void testLoadWithInvalidFile() {
        // Step 1: Create a Configuration object with an invalid file path
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, "/invalid/path/to/file");

        // Step 2: Initialize the TableMapping instance with the configuration
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        // Step 3: Trigger load() via resolve() with dummy input
        List<String> names = new ArrayList<>();
        names.add("host2.example.com");
        List<String> resolvedNames = tableMapping.resolve(names);

        // Step 4: Verify fallback to default rack due to failure in loading the file
        assertEquals(1, resolvedNames.size());
        assertEquals(NetworkTopology.DEFAULT_RACK, resolvedNames.get(0));
    }

    // Test the functionality with a valid topology file
    @Test
    public void testLoadWithValidConfiguration() throws IOException {
        // Step 1: Create a temporary topology mapping file
        File tempFile = File.createTempFile("topology", ".txt");
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("host1.example.com /rack1\n");
            writer.write("host2.example.com /rack2\n");
        }

        // Step 2: Create a Configuration object pointing to the temporary file
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, tempFile.getAbsolutePath());

        // Step 3: Initialize the TableMapping instance with the configuration
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        // Step 4: Prepare the input list and resolve the names
        List<String> names = new ArrayList<>();
        names.add("host1.example.com");
        names.add("host2.example.com");
        names.add("host3.example.com");

        // Step 5: Trigger resolution and verify results
        List<String> resolvedNames = tableMapping.resolve(names);

        assertEquals(3, resolvedNames.size());
        assertEquals("/rack1", resolvedNames.get(0));
        assertEquals("/rack2", resolvedNames.get(1));
        assertEquals(NetworkTopology.DEFAULT_RACK, resolvedNames.get(2));
    }

    // Test the reloadCachedMappings behavior
    @Test
    public void testReloadCachedMappings() throws IOException {
        // Step 1: Create a temporary topology mapping file
        File tempFile = File.createTempFile("topology", ".txt");
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("host1.example.com /rack1\n");
        }

        // Step 2: Create a Configuration object pointing to the temporary file
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, tempFile.getAbsolutePath());

        // Step 3: Initialize the TableMapping instance with the configuration
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        // Step 4: Verify initial resolution
        List<String> names = new ArrayList<>();
        names.add("host1.example.com");
        List<String> resolvedNames = tableMapping.resolve(names);
        assertEquals(1, resolvedNames.size());
        assertEquals("/rack1", resolvedNames.get(0));

        // Step 5: Update topology file with new mapping
        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("host1.example.com /rack1_updated\n");
        }

        // Step 6: Trigger reloadCachedMappings
        tableMapping.reloadCachedMappings();

        // Step 7: Verify resolution reflects updated mapping
        resolvedNames = tableMapping.resolve(names);
        assertEquals(1, resolvedNames.size());
        assertEquals("/rack1_updated", resolvedNames.get(0));
    }
}