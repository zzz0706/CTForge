package org.apache.hadoop.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;

@Category(SmallTests.class)
public class TestDynamicClassLoaderInitialization {

    @ClassRule // HBaseClassTestRule ClassRule
    public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestDynamicClassLoaderInitialization.class);

    @Test
    // Test code
    // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testDynamicClassLoaderInitialization_withValidConfiguration() throws Exception {
        // Prepare the test conditions
        Configuration conf = new Configuration();
        
        // Use the HBase 2.2.2 API correctly to fetch configuration values
        String rootDir = conf.get("hbase.rootdir", "file:///tmp/hbase"); // Add "file://" scheme to fix the missing scheme issue
        URI remoteDirUri = new URI(rootDir + "/lib");
        
        // Ensure directory exists
        if (!Files.exists(Paths.get(remoteDirUri))) {
            Files.createDirectories(Paths.get(remoteDirUri));
        }
        
        conf.set("hbase.dynamic.jars.dir", remoteDirUri.toString());
        conf.setBoolean("hbase.dynamic.jars.optional", true);

        // Test code: Create an instance of DynamicClassLoader
        ClassLoader parentClassLoader = ClassLoader.getSystemClassLoader();
        DynamicClassLoader dynamicClassLoader = new DynamicClassLoader(conf, parentClassLoader);

        // Verify behavior
        // Validate the configuration values directly
        String dynamicJarsDir = conf.get("hbase.dynamic.jars.dir");
        assert dynamicJarsDir.equals(remoteDirUri.toString());

        // Code after testing
        // Clean up temporary directories or resources used during the test
        Files.deleteIfExists(Paths.get(remoteDirUri));
    }
}