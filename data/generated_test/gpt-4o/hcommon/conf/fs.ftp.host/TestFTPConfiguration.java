package org.apache.hadoop.fs.ftp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;

public class TestFTPConfiguration {

    @Test
    public void testFTPHostConfiguration() {
        // Step 1: Read configuration
        Configuration conf = new Configuration();
        String ftpHost = conf.get("fs.ftp.host", "0.0.0.0");

        // Step 2: Validate constraints
        Assert.assertNotNull("FTP host configuration should not be null", ftpHost);
        Assert.assertFalse("FTP host configuration should not be empty", ftpHost.isEmpty());

        try {
            // Validate format using InetSocketAddress (correct Hadoop helper)
            InetSocketAddress addr = NetUtils.createSocketAddr(ftpHost + ":9000");
            Assert.assertNotNull("FTP host should resolve to a valid address", addr.getAddress());
        } catch (IllegalArgumentException ex) {
            Assert.fail("FTP host should be in valid IP/Hostname format");
        }
    }

    @Test
    public void testFTPPortConfiguration() {
        // Step 1: Read configuration
        Configuration conf = new Configuration();
        int ftpPort = conf.getInt("fs.ftp.host.port", 21); // Default FTP port is 21

        // Step 2: Validate range constraints
        Assert.assertTrue("FTP port should be in valid range (1-65535)", ftpPort > 0 && ftpPort <= 65535);
    }

    @Test
    public void testFTPHostAndPortDependency() {
        // Step 1: Read configuration values
        Configuration conf = new Configuration();
        String ftpHost = conf.get("fs.ftp.host", "0.0.0.0");
        int ftpPort = conf.getInt("fs.ftp.host.port", 21); // Default FTP port is 21

        // Step 2: Validate host
        Assert.assertNotNull("FTP host configuration should not be null", ftpHost);
        Assert.assertFalse("FTP host configuration should not be empty", ftpHost.isEmpty());

        try {
            InetSocketAddress addr = NetUtils.createSocketAddr(ftpHost + ":" + ftpPort);
            Assert.assertNotNull("FTP host should resolve to a valid address", addr.getAddress());
        } catch (IllegalArgumentException ex) {
            Assert.fail("FTP host and port should resolve to a valid address");
        }

        // Step 3: Validate port
        Assert.assertTrue("FTP port should be in valid range (1-65535)", ftpPort > 0 && ftpPort <= 65535);
    }

    @Test
    public void testFTPUserAndPasswordConfiguration() {
        // Step 1: Read configuration values
        Configuration conf = new Configuration();
        String ftpHost = conf.get("fs.ftp.host", "0.0.0.0");
        String username = conf.get("fs.ftp.user." + ftpHost, null);
        String password = conf.get("fs.ftp.password." + ftpHost, null);

        // Step 2: Validate username
        Assert.assertNotNull("FTP username should not be null for host: " + ftpHost, username);
        Assert.assertFalse("FTP username should not be empty for host: " + ftpHost, username.isEmpty());

        // Step 3: Validate password
        Assert.assertNotNull("FTP password should not be null for host: " + ftpHost, password);
        Assert.assertFalse("FTP password should not be empty for host: " + ftpHost, password.isEmpty());
    }
}