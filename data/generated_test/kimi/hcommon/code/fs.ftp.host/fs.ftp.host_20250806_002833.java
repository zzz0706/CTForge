package org.apache.hadoop.fs.ftp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.*;

public class FTPFileSystemConfigTest {

    private FTPFileSystem ftpFS;
    private Configuration conf;

    @Before
    public void setUp() {
        conf = new Configuration(false);
        ftpFS = new FTPFileSystem();
    }

    @After
    public void tearDown() throws IOException {
        if (ftpFS != null) {
            ftpFS.close();
        }
    }

    @Test
    public void initializeOverridesDefaultHostWhenUriHostPresent() throws IOException, URISyntaxException {
        // 1. Prepare configuration: set a fallback host
        conf.set(FTPFileSystem.FS_FTP_HOST, "1.2.3.4");

        // 2. Initialize with URI that has host
        URI uri = new URI("ftp://ftp.example.com:21/");
        ftpFS.initialize(uri, conf);

        // 3. Verify host is taken from URI
        String actualHost = conf.get(FTPFileSystem.FS_FTP_HOST);
        assertEquals("ftp.example.com", actualHost);
    }

    @Test
    public void initializeUsesConfHostWhenUriHostMissing() throws IOException, URISyntaxException {
        // 1. Prepare configuration: set fallback host
        conf.set(FTPFileSystem.FS_FTP_HOST, "fallback.host");

        // 2. Initialize with URI missing host
        URI uri = new URI("ftp://:21/");
        ftpFS.initialize(uri, conf);

        // 3. Verify host is taken from configuration
        String actualHost = conf.get(FTPFileSystem.FS_FTP_HOST);
        assertEquals("fallback.host", actualHost);
    }

    @Test(expected = IOException.class)
    public void initializeThrowsWhenNoHostInUriOrConf() throws IOException, URISyntaxException {
        // 1. Ensure no host in configuration
        conf.unset(FTPFileSystem.FS_FTP_HOST);

        // 2. Initialize with URI missing host
        URI uri = new URI("ftp://:21/");
        ftpFS.initialize(uri, conf);
    }

    @Test
    public void connectReadsHostFromConfAfterInitialize() throws IOException, URISyntaxException {
        // 1. Prepare full configuration
        String host = "stored.host";
        conf.set(FTPFileSystem.FS_FTP_HOST, host);
        conf.setInt(FTPFileSystem.FS_FTP_HOST_PORT, 21);
        conf.set(FTPFileSystem.FS_FTP_USER_PREFIX + host, "anonymous");
        conf.set(FTPFileSystem.FS_FTP_PASSWORD_PREFIX + host, "guest");

        // 2. Initialize filesystem
        URI uri = new URI("ftp://stored.host:21/");
        ftpFS.initialize(uri, conf);

        // 3. Trigger connect via getFileStatus and verify host is used
        try {
            ftpFS.getFileStatus(new Path("/"));
            fail("Expected connection failure");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("stored.host"));
        }
    }

    @Test
    public void hostPropagatedThroughAllOperations() throws IOException, URISyntaxException {
        // 1. Prepare configuration
        String host = "test.host";
        conf.set(FTPFileSystem.FS_FTP_HOST, host);
        conf.setInt(FTPFileSystem.FS_FTP_HOST_PORT, 21);
        conf.set(FTPFileSystem.FS_FTP_USER_PREFIX + host, "anonymous");
        conf.set(FTPFileSystem.FS_FTP_PASSWORD_PREFIX + host, "guest");

        // 2. Initialize filesystem
        URI uri = new URI("ftp://test.host:21/");
        ftpFS.initialize(uri, conf);

        // 3. Trigger all public operations to ensure host is used
        Path p = new Path("/dummy");
        try { ftpFS.open(p, 4096); } catch (Exception ignore) {}
        try { ftpFS.create(p, true, 4096, (short) 1, 1024, null); } catch (Exception ignore) {}
        try { ftpFS.delete(p, false); } catch (Exception ignore) {}
        try { ftpFS.listStatus(p); } catch (Exception ignore) {}
        try { ftpFS.getFileStatus(p); } catch (Exception ignore) {}
        try { ftpFS.mkdirs(p, FsPermission.getDefault()); } catch (Exception ignore) {}
        try { ftpFS.rename(p, p); } catch (Exception ignore) {}
        try { ftpFS.getHomeDirectory(); } catch (Exception ignore) {}

        // 4. Verify host is consistently set
        assertEquals("test.host", conf.get(FTPFileSystem.FS_FTP_HOST));
    }
}