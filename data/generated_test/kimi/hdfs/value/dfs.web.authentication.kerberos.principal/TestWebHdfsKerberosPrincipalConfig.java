package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestWebHdfsKerberosPrincipalConfig {

  private Configuration conf;

  @Before
  public void setUp() {
    // 1. Obtain configuration from the configuration file without hard-coding any values
    conf = new Configuration(false);
    conf.addResource("hdfs-site.xml");
    conf.addResource("core-site.xml");
  }

  @After
  public void tearDown() {
    conf = null;
  }

  /**
   * Tests that when security is enabled the configuration
   * {@code dfs.web.authentication.kerberos.principal} is present and non-empty.
   */
  @Test
  public void testWebHdfsKerberosPrincipalRequiredWhenSecurityEnabled()
      throws IOException {

    // 2. Prepare test condition: ensure security is enabled
    boolean securityEnabled = UserGroupInformation.isSecurityEnabled();
    if (!securityEnabled) {
      // Skip test if security is not enabled; we cannot force it in unit test
      return;
    }

    // 3. Test code
    String principal = conf.get(
        DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY);

    assertNotNull(
        "WebHDFS and security are enabled, but configuration property '"
            + DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY
            + "' is not set.",
        principal);

    assertFalse(
        "WebHDFS and security are enabled, but configuration property '"
            + DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY
            + "' is empty.",
        principal.trim().isEmpty());
  }

  /**
   * Tests that when security is disabled the configuration
   * {@code dfs.web.authentication.kerberos.principal} is optional.
   */
  @Test
  public void testWebHdfsKerberosPrincipalOptionalWhenSecurityDisabled()
      throws IOException {

    // 2. Prepare test condition: ensure security is disabled
    boolean securityEnabled = UserGroupInformation.isSecurityEnabled();
    if (securityEnabled) {
      // Skip test if security is enabled
      return;
    }

    // 3. Test code
    String principal = conf.get(
        DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY);

    // 4. Code after testing: principal may be null or empty when security is off
    assertTrue(
        "When security is disabled the principal may be null or empty",
        principal == null || principal.trim().isEmpty());
  }

  /**
   * Tests that the principal follows the expected Kerberos principal format
   * (host-based service principal).
   */
  @Test
  public void testWebHdfsKerberosPrincipalFormat() throws IOException {

    String principal = conf.get(
        DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY);
    if (principal == null || principal.trim().isEmpty()) {
      // Nothing to validate if principal is not configured
      return;
    }

    // 3. Test code
    // Kerberos principal must contain exactly one '@' and at least one '/'
    assertTrue("Kerberos principal must contain exactly one '@'",
        principal.split("@").length == 2);

    assertTrue("Kerberos principal must contain a '/' before '@'",
        principal.split("@")[0].contains("/"));
  }
}