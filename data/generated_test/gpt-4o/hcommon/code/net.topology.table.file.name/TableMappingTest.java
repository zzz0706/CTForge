package org.apache.hadoop.net;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.assertEquals;

public class TableMappingTest {

    // Prepare the input conditions for unit testing.
    @Test
    public void testResolveWithMissingTopologyFile() {
        // Get configuration value using API
        Configuration conf = new Configuration();
        String topologyFileName = conf.get(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, null);

        // Ensure the topology file configuration is unset or invalid
        assertEquals(null, topologyFileName);

        // Initialize TableMapping and test resolve method
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        List<String> nodeNames = Arrays.asList("node1.example.com", "node2.example.com", "node3.example.com");
        List<String> resolvedRackMappings = tableMapping.resolve(nodeNames);

        // Verify all nodes are mapped to '/default-rack' when topology file is not configured or missing
        for (String rack : resolvedRackMappings) {
            assertEquals(NetworkTopology.DEFAULT_RACK, rack);
        }
    }
}