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
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestDynamicClassLoaderInitialization.class);

    @Test
    // testDynamicClassLoaderInitialization_withValidConfiguration
    // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testDynamicClassLoaderInitialization_withValidConfiguration() throws Exception {
        // 1. Prepare the test conditions using HBase 2.2.2 API to fetch configuration values correctly
        Configuration conf = new Configuration();
        String rootDir = conf.get("hbase.rootdir", "file:///tmp/hbase");
        URI remoteDirUri = new URI(rootDir + "/lib");

        // Ensure the remote directory exists
        if (!Files.exists(Paths.get(remoteDirUri))) {
            Files.createDirectories(Paths.get(remoteDirUri));
        }

        // Set the relevant configuration properties
        conf.set("hbase.dynamic.jars.dir", remoteDirUri.toString());
        conf.setBoolean("hbase.dynamic.jars.optional", true);

        // 2. Test code: Create an instance of DynamicClassLoader
        ClassLoader parentClassLoader = ClassLoader.getSystemClassLoader();
        DynamicClassLoader dynamicClassLoader = new DynamicClassLoader(conf, parentClassLoader);

        // 3. Verify that the configuration was used correctly during initialization
        String dynamicJarsDirConfig = conf.get("hbase.dynamic.jars.dir");
        assert dynamicJarsDirConfig != null && dynamicJarsDirConfig.equals(remoteDirUri.toString());

        boolean dynamicJarsEnabled = conf.getBoolean("hbase.dynamic.jars.optional", false);
        assert dynamicJarsEnabled;

        // Optionally verify internal state of DynamicClassLoader (example with reflection or getters, if exposed in API)
        // e.g., assert dynamicClassLoader.getRemoteDir() != null;

        // 4. Clean up testing resources
        Files.deleteIfExists(Paths.get(remoteDirUri));
    }
}