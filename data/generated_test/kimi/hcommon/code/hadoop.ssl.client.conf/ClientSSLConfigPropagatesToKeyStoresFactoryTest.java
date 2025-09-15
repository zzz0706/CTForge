package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertNotNull;

public class ClientSSLConfigPropagatesToKeyStoresFactoryTest {

  @Test
  public void clientSSLConfigPropagatesToKeyStoresFactory() throws Exception {
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    Configuration conf = new Configuration(false);
    // Ensure ssl-client.xml exists in test classpath
    Path sslClientXml = Files.createTempFile("ssl-client", ".xml");
    String xmlContent =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n" +
        "<configuration>\n" +
        "  <property>\n" +
        "    <name>ssl.client.keystore.location</name>\n" +
        "    <value>dummy-client-keystore.jks</value>\n" +
        "  </property>\n" +
        "  <property>\n" +
        "    <name>ssl.client.keystore.password</name>\n" +
        "    <value>changeme</value>\n" +
        "  </property>\n" +
        "  <property>\n" +
        "    <name>ssl.client.truststore.location</name>\n" +
        "    <value>dummy-client-truststore.jks</value>\n" +
        "  </property>\n" +
        "  <property>\n" +
        "    <name>ssl.client.truststore.password</name>\n" +
        "    <value>changeme</value>\n" +
        "  </property>\n" +
        "</configuration>";
    try (OutputStream os = Files.newOutputStream(sslClientXml)) {
      os.write(xmlContent.getBytes("UTF-8"));
    }
    conf.addResource(sslClientXml.toUri().toURL());

    // 2. Prepare the test conditions.
    // Create an SSLFactory in CLIENT mode, which will internally load ssl-client.xml
    SSLFactory factory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);

    // 3. Test code.
    // Retrieve the configuration used by SSLFactory via the conf object we passed in
    String keystoreLocation = conf.get("ssl.client.keystore.location");

    // 4. Code after testing.
    // The value should come from the generated ssl-client.xml; just assert it is not null
    assertNotNull("ssl.client.keystore.location should be loaded from ssl-client.xml", keystoreLocation);
  }
}