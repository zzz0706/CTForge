package org.apache.hadoop.net;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.net.NetworkTopology;
import org.junit.Test;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TableMappingGracefulFallbackWhenFileNotFoundTest {

    private TableMapping mapping;
    private Configuration conf;

    @Before
    public void setUp() {
        conf = new Configuration();
        mapping = new TableMapping();
    }

    @Test
    public void testGracefulFallbackWhenFileNotFound() {
        // 1. Use the HDFS 2.8.5 API to obtain configuration values
        String fileName = conf.get(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY,
                                   "nonexistent-topology-table.file");

        // 2. Prepare test conditions – set the file to a non-existent path
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, fileName);

        // 3. Test code
        mapping.setConf(conf);
        List<String> hosts = Arrays.asList("host1", "host2", "host3");
        List<String> racks = mapping.resolve(hosts);

        // 4. Code after testing
        assertEquals(hosts.size(), racks.size());
        for (String rack : racks) {
            assertEquals(NetworkTopology.DEFAULT_RACK, rack);
        }
    }

    @After
    public void tearDown() {
        if (mapping != null) {
            mapping.reloadCachedMappings(); // Exercise reloadCachedMappings for coverage
        }
    }
}