package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import javax.security.sasl.Sasl;
import java.net.InetAddress;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SaslPropertiesResolverTest {

  @Test
  public void testSinglePrivacyProtection() throws Exception {
    // 1. Instantiate Configuration without explicit set
    Configuration conf = new Configuration();

    // 2. Dynamic expected value calculation
    String[] configuredQops = conf.getTrimmedStrings(
            CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION,
            "authentication"); // default value matches enum AUTHENTICATION
    // Map the first configured qop to SASL QOP value
    String expectedQop;
    switch (configuredQops[0].toLowerCase()) {
      case "authentication":
        expectedQop = "auth";
        break;
      case "integrity":
        expectedQop = "auth-int";
        break;
      case "privacy":
        expectedQop = "auth-conf";
        break;
      default:
        expectedQop = "auth";
    }

    // 3. Prepare resolver and feed configuration
    SaslPropertiesResolver resolver = new SaslPropertiesResolver();
    resolver.setConf(conf);

    // 4. Invoke the method under test
    Map<String, String> props = resolver.getServerProperties(
            InetAddress.getLoopbackAddress());

    // 5. Assertions
    assertEquals(expectedQop, props.get(Sasl.QOP));
  }
}