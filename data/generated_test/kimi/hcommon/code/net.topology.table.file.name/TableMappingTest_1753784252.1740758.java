package org.apache.hadoop.net;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TableMappingTest {

    private File topologyFile;

    @Before
    public void setUp() throws Exception {
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
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY,
                 topologyFile.getAbsolutePath());

        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        List<String> results = tableMapping.resolve(
                Arrays.asList("hostA", "hostB", "unknownHost"));

        assertEquals(Arrays.asList("rack1", "rack2", "/default-rack"), results);
    }
}