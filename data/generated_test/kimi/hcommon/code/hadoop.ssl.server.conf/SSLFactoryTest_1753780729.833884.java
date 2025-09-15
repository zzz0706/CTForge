package org.apache.hadoop.security.ssl;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class SSLFactoryTest {

  @Test
  public void excludeCiphersAreReadFromCustomSslServerXml() throws Exception {
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    Configuration conf = new Configuration();
    
    // 2. Prepare the test conditions.
    // create a temporary ssl-server.xml file with the required property
    File tempDir = new File(System.getProperty("java.io.tmpdir"));
    File sslServerFile = new File(tempDir, "custom-ssl-server.xml");
    try (FileWriter writer = new FileWriter(sslServerFile)) {
      writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
      writer.write("<configuration>\n");
      writer.write("  <property>\n");
      writer.write("    <name>ssl.server.exclude.cipher.list</name>\n");
      writer.write("    <value>TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,TLS_RSA_WITH_AES_128_CBC_SHA256</value>\n");
      writer.write("  </property>\n");
      writer.write("</configuration>\n");
    }
    
    conf.addResource(sslServerFile.toURI().toURL());
    
    List<String> expectedExcludeCiphers = Arrays.asList(
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
        "TLS_RSA_WITH_AES_128_CBC_SHA256");

    // 3. Test code.
    String excludeCiphers = conf.get("ssl.server.exclude.cipher.list", "");
    List<String> actualExcludeCiphers = Arrays.asList(excludeCiphers.split("\\s*,\\s*"));

    // 4. Code after testing.
    assertEquals(expectedExcludeCiphers, actualExcludeCiphers);
    
    // cleanup
    sslServerFile.delete();
  }
}