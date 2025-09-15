package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Test;
import javax.net.ssl.SSLEngine;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SSLFactoryConfigurationTest {

  private SSLFactory sslFactory;

  @After
  public void tearDown() throws Exception {
    if (sslFactory != null) {
      sslFactory.destroy();
    }
  }

  @Test
  public void testCustomProtocolsAreAppliedToSSLContextAndSSLEngine() throws Exception {
    // 1. Use the hdfs 2.8.5 API correctly to obtain configuration values
    Configuration conf = new Configuration();
    // Optionally override the default list for reproducible assertions
    conf.set(SSLFactory.SSL_ENABLED_PROTOCOLS_KEY, "TLSv1.2,TLSv1.1");

    String[] expectedProtocols = conf.getStrings(SSLFactory.SSL_ENABLED_PROTOCOLS_KEY,
                                                 SSLFactory.SSL_ENABLED_PROTOCOLS_DEFAULT);

    // 2. Prepare the test conditions
    sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
    sslFactory.init();

    // 3. Test code
    SSLEngine sslEngine = sslFactory.createSSLEngine();
    assertNotNull("SSLEngine should not be null", sslEngine);

    String[] actualSSLEngineProtocols = sslEngine.getEnabledProtocols();

    // 4. Code after testing
    Set<String> expectedSet = new HashSet<>(Arrays.asList(expectedProtocols));
    Set<String> actualSSLEngineSet = new HashSet<>(Arrays.asList(actualSSLEngineProtocols));

    assertEquals("SSLEngine enabled protocols should match expected",
                 expectedSet, actualSSLEngineSet);
  }
}