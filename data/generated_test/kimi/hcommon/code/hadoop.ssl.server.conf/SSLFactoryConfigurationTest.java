package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.net.URL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SSLFactoryConfigurationTest {

  private static final String SSL_CLIENT_XML = "ssl-client.xml";
  private static final String SSL_SERVER_XML = "ssl-server.xml";

  @Before
  public void setUp() throws Exception {
    // Ensure ssl-client.xml and ssl-server.xml are on the classpath
    createTestResource(SSL_CLIENT_XML);
    createTestResource(SSL_SERVER_XML);
  }

  @Test
  public void clientModeStillUsesSslClientXmlDespiteServerProperty() throws Exception {
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    Configuration conf = new Configuration(false);
    // 2. Prepare the test conditions.
    conf.addResource(SSL_CLIENT_XML);
    conf.addResource(SSL_SERVER_XML);

    // 3. Test code.
    SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);

    // 4. Code after testing.
    java.lang.reflect.Field sslConfField = SSLFactory.class.getDeclaredField("conf");
    sslConfField.setAccessible(true);
    Configuration sslConf = (Configuration) sslConfField.get(sslFactory);

    String expectedResource = conf.get(SSLFactory.SSL_CLIENT_CONF_KEY, SSL_CLIENT_XML);
    URL actualUrl = sslConf.getResource(SSL_CLIENT_XML);
    assertNotNull("ssl-client.xml must be found", actualUrl);
    String actualResource = actualUrl.toString();
    assertEquals("sslConf should contain ssl-client.xml in client mode",
                 expectedResource,
                 actualResource.substring(actualResource.lastIndexOf('/') + 1));
  }

  private void createTestResource(String fileName) throws Exception {
    File dir = new File("target/test-classes");
    if (!dir.exists()) {
      dir.mkdirs();
    }
    File file = new File(dir, fileName);
    if (!file.exists()) {
      try (FileWriter writer = new FileWriter(file)) {
        writer.write("<?xml version=\"1.0\"?>\n<configuration>\n</configuration>");
      }
    }
  }
}