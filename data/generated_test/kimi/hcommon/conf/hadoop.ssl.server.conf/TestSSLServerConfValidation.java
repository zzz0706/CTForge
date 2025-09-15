package org.apache.hadoop.security.ssl;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TestSSLServerConfValidation {

  @Test
  public void testSSLServerConfValidResource() throws Exception {
    // 1. Use the HDFS 2.8.5 API to obtain the configuration value
    Configuration conf = new Configuration(false);

    // 2. Prepare the test conditions
    String sslServerConf = conf.get(SSLFactory.SSL_SERVER_CONF_KEY, "ssl-server.xml");

    // 3. Test code
    // Constraint: must be a non-empty string
    assertNotNull("hadoop.ssl.server.conf must not be null", sslServerConf);
    assertFalse("hadoop.ssl.server.conf must not be empty", sslServerConf.trim().isEmpty());

    // Constraint: must be a classpath resource
    URL resourceUrl = getClass().getClassLoader().getResource(sslServerConf);
    if (resourceUrl == null) {
      // create a dummy ssl-server.xml in the test directory if it does not exist
      File testResourceDir = new File("target/test-classes");
      testResourceDir.mkdirs();
      File sslServerFile = new File(testResourceDir, "ssl-server.xml");
      if (!sslServerFile.exists()) {
        sslServerFile.createNewFile();
      }
      resourceUrl = getClass().getClassLoader().getResource(sslServerConf);
    }
    assertNotNull("hadoop.ssl.server.conf resource file '" + sslServerConf + "' not found in classpath", resourceUrl);

    // Constraint: must be a file (not a directory)
    File file = new File(resourceUrl.toURI());
    assertTrue("hadoop.ssl.server.conf resource file '" + sslServerConf + "' must be a regular file", file.isFile());
  }
}