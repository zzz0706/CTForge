package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.assertEquals;

public class TestDFSInternalNameServices {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getInternalNameServices_with_empty_internal_nameservices() {
        // Step 1: Prepare the test conditions
        Configuration conf = new Configuration(); 
        String nameservice1 = "nameservice1";
        String nameservice2 = "nameservice2";

        conf.set("dfs.nameservices", nameservice1 + "," + nameservice2);

        // Step 2: Test the functionality
        Collection<String> internalNameServices = DFSUtil.getInternalNameServices(conf);

        // Step 3: Verify the results
        assertEquals(2, internalNameServices.size());
        assertEquals(true, internalNameServices.contains(nameservice1));
        assertEquals(true, internalNameServices.contains(nameservice2));
    }
}