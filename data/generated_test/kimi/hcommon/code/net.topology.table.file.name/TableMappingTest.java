package org.apache.hadoop.net;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetworkTopology;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TableMappingTest {

    @Test
    public void testDefaultRackFallbackWhenFileNameNotSet() {
        // 1. Instantiate Configuration without setting net.topology.table.file.name
        Configuration conf = new Configuration();

        // 2. Build TableMapping instance and inject the configuration
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);

        // 3. Call resolve(List<String>) with arbitrary host names
        List<String> hosts = Arrays.asList("host1", "host2.example.com", "192.168.1.1");
        List<String> actualRacks = tableMapping.resolve(hosts);

        // 4. Compute expected rack (NetworkTopology.DEFAULT_RACK)
        String expectedRack = NetworkTopology.DEFAULT_RACK;

        // 5. Assert all returned racks equal the default rack
        assertEquals(hosts.size(), actualRacks.size());
        for (String actual : actualRacks) {
            assertEquals(expectedRack, actual);
        }
    }
}