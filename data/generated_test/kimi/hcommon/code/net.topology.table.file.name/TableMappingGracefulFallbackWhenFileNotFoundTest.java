package org.apache.hadoop.net;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.net.NetworkTopology;
import org.junit.Test;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TableMappingGracefulFallbackWhenFileNotFoundTest {

    @Test
    public void testGracefulFallbackWhenFileNotFound() {
        // 1. Obtain configuration values
        Configuration conf = new Configuration();
        String fileName = conf.get(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY,
                                   "nonexistent-topology-table.file");

        // 2. Prepare test conditions – set the file to a non-existent path
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, fileName);

        // 3. Test code
        TableMapping mapping = new TableMapping();
        mapping.setConf(conf);

        List<String> hosts = Arrays.asList("host1", "host2", "host3");
        List<String> racks = mapping.resolve(hosts);

        // 4. Assertions
        assertEquals(hosts.size(), racks.size());
        for (String rack : racks) {
            assertEquals(NetworkTopology.DEFAULT_RACK, rack);
        }
    }
}