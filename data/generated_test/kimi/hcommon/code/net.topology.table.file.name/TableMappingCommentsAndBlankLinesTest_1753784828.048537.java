package org.apache.hadoop.net;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class TableMappingCommentsAndBlankLinesTest {

    private File topologyFile;
    private Configuration conf;

    @Before
    public void setUp() throws IOException {
        // 1. Create a temporary topology file that contains comments and blank lines
        topologyFile = File.createTempFile("topology", ".txt");
        topologyFile.deleteOnExit();
        try (FileWriter writer = new FileWriter(topologyFile)) {
            writer.write("# This is a comment line\n");
            writer.write("\n");                 // blank line
            writer.write("   \n");            // blank line with spaces
            writer.write("# another comment\n");
            writer.write("hostE rack6\n");
            writer.write("\n");                 // trailing blank line
        }

        // 2. Configure the Hadoop Configuration object to use the temporary file
        conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY,
                 topologyFile.getAbsolutePath());
    }

    @After
    public void tearDown() {
        if (topologyFile != null) {
            topologyFile.delete();
        }
    }

    @Test
    public void testCommentsAndBlankLinesSkipped() throws Exception {
        // 3. Build TableMapping instance and inject configuration
        TableMapping mapping = new TableMapping();
        mapping.setConf(conf);

        // 4. Invoke resolve(List<String>) to exercise load() and resolve()
        List<String> result = mapping.resolve(Arrays.asList("hostE"));

        // 5. Verify that only the valid mapping is loaded and returned
        assertEquals("Expected exactly one result", 1, result.size());
        assertEquals("hostE should map to rack6", "rack6", result.get(0));
    }

    @Test
    public void testReloadCachedMappings() throws Exception {
        // Prepare a second mapping file to test reload
        File newFile = File.createTempFile("newTopology", ".txt");
        newFile.deleteOnExit();
        try (FileWriter writer = new FileWriter(newFile)) {
            writer.write("hostE rack99\n"); // new mapping
        }

        // 1. Initial load
        TableMapping mapping = new TableMapping();
        mapping.setConf(conf);
        List<String> firstResult = mapping.resolve(Arrays.asList("hostE"));
        assertEquals("Initial mapping should be rack6", "rack6", firstResult.get(0));

        // 2. Change configuration to point to new file
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY,
                 newFile.getAbsolutePath());
        mapping.setConf(conf);

        // 3. Trigger reload
        mapping.reloadCachedMappings();

        // 4. Verify new mapping is used
        List<String> secondResult = mapping.resolve(Arrays.asList("hostE"));
        assertEquals("After reload mapping should be rack99", "rack99", secondResult.get(0));

        // 5. Clean up
        newFile.delete();
    }
}