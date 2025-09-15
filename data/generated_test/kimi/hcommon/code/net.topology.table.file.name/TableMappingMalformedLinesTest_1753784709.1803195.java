package org.apache.hadoop.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.TableMapping;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TableMappingMalformedLinesTest {

    private File topologyFile;
    private TableMapping mapping;

    @Before
    public void setUp() throws Exception {
        topologyFile = File.createTempFile("topology", ".txt");
        topologyFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(topologyFile)) {
            writer.write("hostC /rack3\n");
            writer.write("invalidLineWithOneColumn\n");
        }

        // Ensure configuration is obtained via the public key constant
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY,
                 topologyFile.getAbsolutePath());

        mapping = new TableMapping();
        mapping.setConf(conf);
    }

    @After
    public void tearDown() {
        if (topologyFile != null) {
            topologyFile.delete();
        }
    }

    @Test
    public void testMalformedLinesIgnoredWithWarning() throws Exception {
        // 2. Prepare the test conditions: file already written in setUp()
        // 3. Test code: call resolve with both valid and invalid host names
        List<String> actual = mapping.resolve(
                Arrays.asList("hostC", "invalidLineWithOneColumn"));

        // 4. Code after testing: assertions
        assertEquals(Arrays.asList("/rack3", NetworkTopology.DEFAULT_RACK), actual);
    }

    @Test
    public void testReloadCachedMappings() throws Exception {
        // 2. Prepare new content and force reload
        try (FileWriter writer = new FileWriter(topologyFile)) {
            writer.write("hostC /rack3\n");
            writer.write("hostD /rack4\n");
        }

        // 3. Test code: reload mappings and resolve
        mapping.reloadCachedMappings();
        List<String> actual = mapping.resolve(Arrays.asList("hostC", "hostD"));

        // 4. Code after testing: verify new mappings are loaded
        assertEquals(Arrays.asList("/rack3", "/rack4"), actual);
    }

    @Test
    public void testMissingFileFallsBackToDefault() throws Exception {
        // 2. Prepare test condition: delete the file
        topologyFile.delete();

        // 3. Test code: reload and resolve
        mapping.reloadCachedMappings();
        List<String> actual = mapping.resolve(Arrays.asList("hostC"));

        // 4. Code after testing: verify fallback
        assertEquals(Arrays.asList(NetworkTopology.DEFAULT_RACK), actual);
    }
}