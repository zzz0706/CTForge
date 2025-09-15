package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestSSLFactoryConfigValidation {

  private Configuration conf;

  @Before
  public void setUp() {
    conf = new Configuration(false);
    // Do NOT set any configuration values in the test code.
  }

  @After
  public void tearDown() {
    conf.clear();
  }

  /**
   * Tests that the configuration value for hadoop.ssl.enabled.protocols
   * satisfies the following constraints:
   * 1. It must be a comma-separated list of valid SSL/TLS protocol names.
   * 2. Each protocol must be one of the standard SSL/TLS protocol strings.
   * 3. If hadoop.ssl.enabled is false, the protocols configuration is ignored.
   * 4. The configuration must not be null when hadoop.ssl.enabled is true.
   */
  @Test
  public void testSSLProtocolsConfigurationValidity() {
    // Load configuration from core-site.xml or hadoop configuration files
    conf.addResource("core-site.xml");
    conf.addResource("core-default.xml");

    boolean sslEnabled = conf.getBoolean("hadoop.ssl.enabled", false);
    String[] enabledProtocols = conf.getStrings("hadoop.ssl.enabled.protocols",
        SSLFactory.SSL_ENABLED_PROTOCOLS_DEFAULT);

    if (sslEnabled) {
      assertNotNull("hadoop.ssl.enabled.protocols must not be null when SSL is enabled",
          enabledProtocols);
      assertTrue("hadoop.ssl.enabled.protocols must contain at least one protocol when SSL is enabled",
          enabledProtocols.length > 0);

      for (String protocol : enabledProtocols) {
        assertNotNull("Protocol in hadoop.ssl.enabled.protocols must not be null", protocol);
        assertFalse("Protocol must not be empty string", protocol.trim().isEmpty());
        
        // Check if protocol is a valid SSL/TLS protocol name
        String upperProtocol = protocol.trim().toUpperCase();
        assertTrue("Protocol '" + protocol + "' is not a valid SSL/TLS protocol name",
            upperProtocol.startsWith("SSL") || upperProtocol.startsWith("TLS"));
      }
    }
  }

  /**
   * Tests that the default value for hadoop.ssl.enabled.protocols is valid
   * when hadoop.ssl.enabled is true.
   */
  @Test
  public void testDefaultSSLProtocolsWhenSSLEnabled() {
    conf.addResource("core-site.xml");
    conf.addResource("core-default.xml");

    // Simulate SSL being enabled to test default protocols
    conf.setBoolean("hadoop.ssl.enabled", true);
    String[] enabledProtocols = conf.getStrings("hadoop.ssl.enabled.protocols",
        SSLFactory.SSL_ENABLED_PROTOCOLS_DEFAULT);

    assertNotNull("Default SSL protocols must not be null", enabledProtocols);
    assertEquals("Default protocols should include 4 protocols", 4, enabledProtocols.length);
    
    String[] expectedProtocols = {"TLSv1", "SSLv2Hello", "TLSv1.1", "TLSv1.2"};
    for (String expected : expectedProtocols) {
      boolean found = false;
      for (String actual : enabledProtocols) {
        if (expected.equals(actual)) {
          found = true;
          break;
        }
      }
      assertTrue("Expected protocol '" + expected + "' not found in default protocols", found);
    }
  }
}