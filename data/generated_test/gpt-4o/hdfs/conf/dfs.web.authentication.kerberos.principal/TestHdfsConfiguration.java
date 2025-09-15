package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Test;

public class TestHdfsConfiguration {

  @Test
  public void testDfsWebAuthenticationKerberosPrincipal() {
    // Step 1: Load HDFS configuration.
    Configuration conf = new Configuration();
    
    // Step 2: Retrieve the configuration value using the HDFS API.
    String principal = conf.get(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY);

    // Step 3: Prepare and validate the test conditions.

    // Condition 1: The configuration should not be empty if security is enabled.
    if (UserGroupInformation.isSecurityEnabled()) {
      Assert.assertNotNull(
          "WebHDFS and security are enabled, but dfs.web.authentication.kerberos.principal is not set.", 
          principal);
      Assert.assertFalse(
          "WebHDFS and security are enabled, but dfs.web.authentication.kerberos.principal is empty.", 
          principal.isEmpty());
    } 
    
    // Condition 2: If set, it should propagate correctly to dependent configurations.
    String internalSpnegoPrincipal = conf.get(DFSConfigKeys.DFS_NAMENODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY);
    if (principal != null && !principal.isEmpty()) {
      Assert.assertEquals("dfs.namenode.kerberos.internal.spnego.principal should match dfs.web.authentication.kerberos.principal",
          principal, internalSpnegoPrincipal);
    }
  }
}