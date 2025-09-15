package org.apache.hadoop.fs.ftp;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestFTPHostConfigValidation {

  private Configuration conf;

  @Before
  public void setUp() {
    conf = new Configuration();
  }

  @After
  public void tearDown() {
    conf = null;
  }

  /**
   * Validate that the configured value for fs.ftp.host is a syntactically
   * correct host name or IP address and that the companion port is valid.
   * The test does NOT set any value; it only validates whatever is present
   * in the loaded configuration files.
   */
  @Test
  public void testFtpHostIsValidHostNameOrIp() throws Exception {
    String host = conf.get("fs.ftp.host", "0.0.0.0");
    int port = conf.getInt("fs.ftp.host.port", 21);

    // host must be non-empty
    assertNotNull("fs.ftp.host must not be null", host);
    assertFalse("fs.ftp.host must not be empty", host.trim().isEmpty());

    // host must be resolvable
    try {
      InetAddress.getByName(host);
    } catch (UnknownHostException e) {
      fail("fs.ftp.host value '" + host + "' is not a valid host name or IP address");
    }

    // port must be in valid range
    assertTrue("fs.ftp.host.port must be between 1 and 65535", port > 0 && port <= 65535);
  }
}