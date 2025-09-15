package org.apache.hadoop.net;   

import org.apache.hadoop.conf.Configuration;       
import org.apache.hadoop.net.TableMapping;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TableMappingTest {       
    // Test when configuration is blank or not set
    @Test
    public void testLoadWithBlankConfiguration() {
        // Step 1: Create a Configuration object without setting "net.topology.table.file.name"
        Configuration conf = new Configuration();
        
        // Step 2: Initialize the TableMapping instance with the configuration
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);
        
        // Step 3: Prepare the input list for resolution
        List<String> inputNames = new ArrayList<>();
        inputNames.add("host1.example.com");
        
        // Step 4: Call the resolve method
        List<String> resolvedNames = tableMapping.resolve(inputNames);
        
        // Step 5: Verify fallback to default rack due to blank configuration
        assertEquals(1, resolvedNames.size());
        assertEquals(NetworkTopology.DEFAULT_RACK, resolvedNames.get(0));
    }
    
    // Test when configuration points to an invalid file
    @Test
    public void testLoadWithInvalidFile() {
        // Step 1: Create a Configuration object with an invalid file path
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, "/invalid/path/to/file");
        
        // Step 2: Initialize the TableMapping instance with the configuration
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);
        
        // Step 3: Prepare the input list for resolution
        List<String> inputNames = new ArrayList<>();
        inputNames.add("host2.example.com");
        
        // Step 4: Call the resolve method
        List<String> resolvedNames = tableMapping.resolve(inputNames);
        
        // Step 5: Verify fallback to default rack due to failure in loading the file
        assertEquals(1, resolvedNames.size());
        assertEquals(NetworkTopology.DEFAULT_RACK, resolvedNames.get(0));
    }
    
    // Test loading topology table from a valid configuration file
    @Test
    public void testLoadWithValidConfiguration() throws IOException {
        // Step 1: Create a temporary topology mapping file
        File tempFile = File.createTempFile("topology", ".txt");
        tempFile.deleteOnExit();
        
        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("host1.example.com /rack1\n");
            writer.write("host2.example.com /rack2\n");
        }
        
        // Step 2: Create a Configuration object pointing to the valid file
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, tempFile.getAbsolutePath());
        
        // Step 3: Initialize the TableMapping instance with the configuration
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);
        
        // Step 4: Prepare the input list for resolution
        List<String> inputNames = new ArrayList<>();
        inputNames.add("host1.example.com");
        inputNames.add("host2.example.com");
        inputNames.add("host3.example.com");
        
        // Step 5: Call the resolve method
        List<String> resolvedNames = tableMapping.resolve(inputNames);
        
        // Step 6: Verify the resolved rack locations from the file
        assertEquals(3, resolvedNames.size());
        assertEquals("/rack1", resolvedNames.get(0));
        assertEquals("/rack2", resolvedNames.get(1));
        assertEquals(NetworkTopology.DEFAULT_RACK, resolvedNames.get(2));
    }
    
    // Test the reloadCachedMappings method
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
        
        // Step 4: Prepare the input list for resolution
        List<String> inputNames = new ArrayList<>();
        inputNames.add("host1.example.com");
        List<String> resolvedNames = tableMapping.resolve(inputNames);
        
        // Step 5: Verify resolution from initial mapping
        assertEquals(1, resolvedNames.size());
        assertEquals("/rack1", resolvedNames.get(0));
        
        // Step 6: Update the topology file
        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("host1.example.com /rack1_updated\n");
        }
        
        // Step 7: Trigger reloadCachedMappings
        tableMapping.reloadCachedMappings();
        
        // Step 8: Verify resolution from updated mapping
        resolvedNames = tableMapping.resolve(inputNames);
        assertEquals(1, resolvedNames.size());
        assertEquals("/rack1_updated", resolvedNames.get(0));
    }
}