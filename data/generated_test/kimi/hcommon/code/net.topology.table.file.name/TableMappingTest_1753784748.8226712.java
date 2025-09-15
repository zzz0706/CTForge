package org.apache.hadoop.net;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TableMappingTest {

    @Test
    public void testReloadMappingsUpdatesCache() throws Exception {
        // 1. Obtain configuration via Configuration#get
        Configuration conf = new Configuration();
        String key = CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY;

        // 2. Prepare the test conditions
        File topologyFile = File.createTempFile("topology", ".txt");
        topologyFile.deleteOnExit();
        String filePath = topologyFile.getAbsolutePath();

        // Write initial content
        try (FileWriter writer = new FileWriter(topologyFile)) {
            writer.write("hostD rack4\n");
        }

        // Configure the topology file path
        conf.set(key, filePath);

        // Build TableMapping and trigger initial load
        TableMapping mapping = new TableMapping();
        mapping.setConf(conf);
        List<String> firstResult = mapping.resolve(Arrays.asList("hostD"));

        // Overwrite file with new content
        try (FileWriter writer = new FileWriter(topologyFile)) {
            writer.write("hostD rack5\n");
        }

        // 3. Test code: reload mappings
        mapping.reloadCachedMappings();

        // 4. Verify updated cache
        List<String> secondResult = mapping.resolve(Arrays.asList("hostD"));
        assertEquals(Arrays.asList("rack4"), firstResult);
        assertEquals(Arrays.asList("rack5"), secondResult);
    }
}