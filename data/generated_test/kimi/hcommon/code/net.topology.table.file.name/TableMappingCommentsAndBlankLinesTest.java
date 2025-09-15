package org.apache.hadoop.net;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TableMappingCommentsAndBlankLinesTest {

    @Test
    public void testCommentsAndBlankLinesSkipped() throws Exception {
        // 1. Create temporary topology file with comments and blank lines
        File tempFile = File.createTempFile("topology", ".txt");
        tempFile.deleteOnExit();
        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("#comment\n");
            writer.write("\n");
            writer.write("hostE rack6\n");
        }

        // 2. Configure using Configuration API
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, tempFile.getAbsolutePath());

        // 3. Build TableMapping instance
        TableMapping mapping = new TableMapping();
        mapping.setConf(conf);

        // 4. Invoke resolve method
        List<String> result = mapping.resolve(Arrays.asList("hostE"));

        // 5. Verify expected result
        assertEquals(1, result.size());
        assertEquals("rack6", result.get(0));
    }
}