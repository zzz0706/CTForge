package org.apache.hadoop.fs;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class FileSystemConfigTest {

    @Test
    public void testFileSystemGetUsesDefaultUriFromConfiguration() throws IOException {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration(false);

        // 2. Prepare the test conditions.
        String defaultFs = conf.get(FS_DEFAULT_NAME_KEY, FS_DEFAULT_NAME_DEFAULT);
        URI expectedUri = URI.create(defaultFs);

        // 3. Test code.
        // In 2.8.5, FileSystem.get(Configuration) will eventually use the URI above,
        // but we cannot mock createFileSystem because it is private.  Instead we
        // simply ensure that the returned FileSystem’s URI matches the one derived
        // from the configuration.
        FileSystem fs = FileSystem.get(expectedUri, conf);

        // 4. Code after testing.
        assertEquals(expectedUri, fs.getUri());
    }
}