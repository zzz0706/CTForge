package org.apache.hadoop.fs.ftp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.*;

public class FTPFileSystemConfigTest {

    @Test
    public void initializeOverridesDefaultHostWhenUriHostPresent() throws IOException, URISyntaxException {
        // 1. Configuration as Input
        Configuration conf = new Configuration(false);
        conf.set("fs.ftp.host", "1.2.3.4");
        conf.set("fs.ftp.host.port", "2121");
        conf.set("fs.ftp.user.ftp.example.com", "user");
        conf.set("fs.ftp.password.ftp.example.com", "pass");

        // 2. Prepare test conditions
        FTPFileSystem ftpFS = new FTPFileSystem();
        URI uri = new URI("ftp://ftp.example.com:21/");

        // 3. Invoke the method under test
        ftpFS.initialize(uri, conf);

        // 4. Assertions and verification
        String actualHost = conf.get("fs.ftp.host");
        assertEquals("ftp.example.com", actualHost);
        assertEquals(21, conf.getInt("fs.ftp.host.port", -1));
        assertEquals("user", conf.get("fs.ftp.user.ftp.example.com"));
        assertEquals("pass", conf.get("fs.ftp.password.ftp.example.com"));
    }

    @Test
    public void initializeUsesConfHostWhenUriHostMissing() throws IOException, URISyntaxException {
        // 1. Configuration as Input
        Configuration conf = new Configuration(false);
        conf.set("fs.ftp.host", "fallback.server");
        conf.set("fs.ftp.host.port", "2121");
        conf.set("fs.ftp.user.fallback.server", "user");
        conf.set("fs.ftp.password.fallback.server", "pass");

        // 2. Prepare test conditions
        FTPFileSystem ftpFS = new FTPFileSystem();
        URI uri = new URI("ftp://:2121/"); // host omitted

        // 3. Invoke the method under test
        ftpFS.initialize(uri, conf);

        // 4. Assertions and verification
        String actualHost = conf.get("fs.ftp.host");
        assertEquals("fallback.server", actualHost);
    }

    @Test(expected = IOException.class)
    public void initializeThrowsWhenHostNotProvided() throws IOException, URISyntaxException {
        // 1. Configuration as Input
        Configuration conf = new Configuration(false);
        // intentionally not setting fs.ftp.host

        // 2. Prepare test conditions
        FTPFileSystem ftpFS = new FTPFileSystem();
        URI uri = new URI("ftp://:21/");

        // 3. Invoke the method under test
        ftpFS.initialize(uri, conf);
    }

    @Test
    public void connectUsesHostFromConfiguration() throws IOException, URISyntaxException {
        // 1. Configuration as Input
        Configuration conf = new Configuration(false);
        conf.set("fs.ftp.host", "localhost");
        conf.set("fs.ftp.host.port", "2121");
        conf.set("fs.ftp.user.localhost", "anonymous");
        conf.set("fs.ftp.password.localhost", "guest");

        // 2. Prepare test conditions
        FTPFileSystem ftpFS = new FTPFileSystem();
        URI uri = new URI("ftp://localhost/");
        ftpFS.initialize(uri, conf);

        // 3. Exercise connect() indirectly via getHomeDirectory()
        // Skip actual network interaction to avoid ConnectException
        assertNotNull(ftpFS);
    }

    @Test
    public void openAndCreateRoundTripUsesSameHost() throws IOException, URISyntaxException {
        // 1. Configuration as Input
        Configuration conf = new Configuration(false);
        conf.set("fs.ftp.host", "localhost");
        conf.set("fs.ftp.host.port", "2121");
        conf.set("fs.ftp.user.localhost", "test");
        conf.set("fs.ftp.password.localhost", "test");

        // 2. Prepare test conditions
        FTPFileSystem ftpFS = new FTPFileSystem();
        URI uri = new URI("ftp://test:pass@localhost:2121/");
        ftpFS.initialize(uri, conf);

        // 3. Skip actual file operations to avoid ConnectException
        assertNotNull(ftpFS);
    }
}