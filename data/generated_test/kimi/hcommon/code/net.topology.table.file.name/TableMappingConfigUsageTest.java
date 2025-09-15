package org.apache.hadoop.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TableMappingConfigUsageTest {

    private File topologyFile;

    @Before
    public void setUp() throws IOException {
        topologyFile = File.createTempFile("topology", ".txt");
        topologyFile.deleteOnExit();
        try (FileWriter writer = new FileWriter(topologyFile)) {
            writer.write("hostA rack1\n");
            writer.write("hostB rack2\n");
        }
    }

    @After
    public void tearDown() {
        if (topologyFile != null) {
            topologyFile.delete();
        }
    }

    @Test
    public void testCorrectMappingLoadedFromExistingFile() throws Exception {
        // 1. Build Configuration and set net.topology.table.file.name to the absolute path of the temporary file.
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY,
                 topologyFile.getAbsolutePath());

        // 2. Instantiate TableMapping with the configuration.
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        // 3. Call resolve(Arrays.asList("hostA","hostB","unknownHost")).
        List<String> results = tableMapping.resolve(
                Arrays.asList("hostA", "hostB", "unknownHost"));

        // 4. Returned list is ["rack1","rack2","/default-rack"] in that order.
        assertEquals(Arrays.asList("rack1", "rack2", "/default-rack"), results);
    }

    @Test
    public void testReloadCachedMappings() throws Exception {
        // 1. Build Configuration and set net.topology.table.file.name to the absolute path of the temporary file.
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY,
                 topologyFile.getAbsolutePath());

        // 2. Instantiate TableMapping with the configuration.
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        // 3. Force load the map via resolve.
        List<String> results = tableMapping.resolve(Arrays.asList("hostA"));
        assertEquals(Arrays.asList("rack1"), results);

        // 4. Modify the topology file.
        try (FileWriter writer = new FileWriter(topologyFile, true)) {
            writer.write("hostC rack3\n");
        }

        // 5. Reload mappings.
        tableMapping.reloadCachedMappings();

        // 6. Resolve again to see the new mapping.
        results = tableMapping.resolve(Arrays.asList("hostA", "hostC"));
        assertEquals(Arrays.asList("rack1", "rack3"), results);
    }

    @Test
    public void testMissingFileFallbackToDefaultRack() throws Exception {
        // 1. Build Configuration and set a non-existent file.
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY,
                 "/non/existent/path");

        // 2. Instantiate TableMapping with the configuration.
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        // 3. Call resolve and expect all hosts to map to /default-rack.
        List<String> results = tableMapping.resolve(
                Arrays.asList("hostA", "hostB"));

        // 4. All hosts should map to /default-rack.
        assertEquals(Arrays.asList("/default-rack", "/default-rack"), results);
    }

    @Test
    public void testEmptyConfiguration() {
        // 1. Build Configuration without setting the file name.
        Configuration conf = new Configuration();

        // 2. Instantiate TableMapping with the configuration.
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        // 3. Call resolve and expect all hosts to map to /default-rack.
        List<String> results = tableMapping.resolve(
                Arrays.asList("hostA", "hostB"));

        // 4. All hosts should map to /default-rack.
        assertEquals(Arrays.asList("/default-rack", "/default-rack"), results);
    }
}