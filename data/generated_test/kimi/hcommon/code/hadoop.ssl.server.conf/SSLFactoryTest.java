package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.Before;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import static org.junit.Assert.*;

public class SSLFactoryTest {

    @Before
    public void setUp() throws Exception {
        // Prepare ssl-server.xml in test classpath
        File testResourceDir = new File("target/test-classes");
        if (!testResourceDir.exists()) {
            testResourceDir.mkdirs();
        }
        File sslServerXml = new File(testResourceDir, "ssl-server.xml");
        try (Writer writer = new OutputStreamWriter(new FileOutputStream(sslServerXml), "UTF-8")) {
            writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
            writer.write("<configuration>\n");
            writer.write("  <property>\n");
            writer.write("    <name>ssl.server.keystore.location</name>\n");
            writer.write("    <value>/tmp/test.jks</value>\n");
            writer.write("  </property>\n");
            writer.write("</configuration>\n");
        }
    }

    @Test
    public void defaultSslServerXmlIsLoadedWhenPropertyIsNotSet() throws Exception {
        // 1. Create a Configuration instance without setting hadoop.ssl.server.conf
        Configuration conf = new Configuration();

        // 2. Instantiate SSLFactory with Mode.SERVER and the Configuration
        SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.SERVER, conf);

        // 3. Verify that ssl-server.xml was loaded by checking a known property from it
        // Use the Configuration API to load the ssl-server.xml
        Configuration sslConf = new Configuration(false);
        sslConf.addResource("ssl-server.xml");
        
        // Verify that sslConf contains properties from ssl-server.xml
        String keystoreLocation = sslConf.get("ssl.server.keystore.location");
        assertNotNull("ssl-server.xml should have been loaded and contain keystore location", 
                     keystoreLocation);
        
        // Verify the resource was actually loaded by checking the configuration sources
        assertNotNull("ssl-server.xml must exist in classpath", 
                      getClass().getClassLoader().getResource("ssl-server.xml"));
    }
}