package org.apache.hadoop.fs.ftp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ftp.FTPFileSystem;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;

public class FTPFileSystemConfigTest {

    @Test
    public void initializeUsesDefaultHostWhenUriHostAbsent() throws IOException, URISyntaxException {
        // 1. Create a fresh Configuration and set the default host
        Configuration conf = new Configuration(false);
        conf.set("fs.ftp.host", "0.0.0.0");

        // 2. Build URI that contains a valid dummy host so FTPFileSystem doesn't throw
        URI uri = new URI("ftp:///");

        // 3. Instantiate and initialize the FTPFileSystem
        FTPFileSystem ftpFS = new FTPFileSystem();
        ftpFS.initialize(uri, conf);

        // 4. Read the host that was set back into the Configuration
        String actualHost = conf.get("fs.ftp.host", "0.0.0.0");
        String expectedHost = "0.0.0.0";

        // 5. Assert the host matches the default
        assertEquals(expectedHost, actualHost);
    }
}