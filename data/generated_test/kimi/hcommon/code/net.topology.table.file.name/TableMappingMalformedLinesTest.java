package org.apache.hadoop.net;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.TableMapping;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TableMappingMalformedLinesTest {

    private File topologyFile;

    @Before
    public void setUp() throws Exception {
        topologyFile = File.createTempFile("topology", ".txt");
        topologyFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(topologyFile)) {
            writer.write("hostC /rack3\n");
            writer.write("invalidLineWithOneColumn\n");
        }
    }

    @After
    public void tearDown() {
        if (topologyFile != null) {
            topologyFile.delete();
        }
    }

    @Test
    public void testMalformedLinesIgnoredWithWarning() throws Exception {
        // 1. Configuration as Input
        Configuration conf = new Configuration();
        conf.set("net.topology.table.file.name", topologyFile.getAbsolutePath());

        // 2. Dynamic Expected Value Calculation
        String expectedRackHostC = "/rack3";
        String expectedRackInvalid = NetworkTopology.DEFAULT_RACK;

        // 3. Invoke the Method Under Test
        TableMapping mapping = new TableMapping();
        mapping.setConf(conf);
        List<String> actual = mapping.resolve(Arrays.asList("hostC", "invalidLineWithOneColumn"));

        // 4. Assertions and Verification
        assertEquals(Arrays.asList(expectedRackHostC, expectedRackInvalid), actual);
    }
}