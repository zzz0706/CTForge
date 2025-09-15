package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FileSystemConfigTest {

    @Test
    public void FileSystem_getServerDefaults_returnsBytesPerChecksumFromConfiguration() throws Exception {
        // 1. Instantiate Configuration; rely on test-resource overrides or defaults
        Configuration conf = new Configuration();

        // 2. Dynamically compute expected value from the same Configuration
        int expectedBytesPerChecksum = conf.getInt("io.bytes.per.checksum", 512);

        // 3. Instantiate concrete FileSystem subclass that uses the Configuration
        RawLocalFileSystem fs = new RawLocalFileSystem();
        fs.initialize(fs.getWorkingDirectory().toUri(), conf);

        // 4. Invoke method under test
        FsServerDefaults defaults = fs.getServerDefaults();

        // 5. Assert the returned value matches the configuration
        assertEquals(expectedBytesPerChecksum, defaults.getBytesPerChecksum());
    }
}