package org.apache.hadoop.fs.ftp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;

public class FTPFileSystemConfigTest {

    @Test
    public void initializeOverridesDefaultHostWhenUriHostPresent() throws IOException, URISyntaxException {
        // 1. Configuration as Input
        Configuration conf = new Configuration(false);
        conf.set("fs.ftp.host", "1.2.3.4");

        // 2. Dynamic Expected Value Calculation
        String expectedHost = "ftp.example.com";

        // 3. Prepare test conditions
        FTPFileSystem ftpFS = new FTPFileSystem();
        URI uri = new URI("ftp://ftp.example.com:21/");

        // 4. Invoke the method under test
        ftpFS.initialize(uri, conf);

        // 5. Assertions and verification
        String actualHost = conf.get("fs.ftp.host");
        assertEquals(expectedHost, actualHost);
    }
}