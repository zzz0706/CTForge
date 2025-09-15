package org.apache.hadoop.conf;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

public class TestIOFileBufferSizeConfig {

  @Test
  public void testValidIOFileBufferSize() throws Exception {
    Configuration conf = new Configuration(false);
    // Simulate loading a configuration file that contains a valid value
    String validConf =
        "<?xml version=\"1.0\"?>\n" +
        "<configuration>\n" +
        "  <property>\n" +
        "    <name>io.file.buffer.size</name>\n" +
        "    <value>8192</value>\n" +
        "  </property>\n" +
        "</configuration>";
    try (InputStream is = new ByteArrayInputStream(validConf.getBytes(StandardCharsets.UTF_8))) {
      conf.addResource(is);
    }
    int bufferSize = conf.getInt(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
                                 CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);
    // In Hadoop 2.8.5 the Configuration object only returns the raw value, no validation is performed.
    // So we simply assert the value we set.
    assertEquals("io.file.buffer.size must be 8192", 8192, bufferSize);
  }

  @Test
  public void testNegativeIOFileBufferSize() throws Exception {
    Configuration conf = new Configuration(false);
    // When negative value is provided, Configuration returns that exact value
    String invalidConf =
        "<?xml version=\"1.0\"?>\n" +
        "<configuration>\n" +
        "  <property>\n" +
        "    <name>io.file.buffer.size</name>\n" +
        "    <value>-1024</value>\n" +
        "  </property>\n" +
        "</configuration>";
    try (InputStream is = new ByteArrayInputStream(invalidConf.getBytes(StandardCharsets.UTF_8))) {
      conf.addResource(is);
    }
    int bufferSize = conf.getInt(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
                                 CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);
    // Expect the value we set to be returned
    assertEquals("io.file.buffer.size must be -1024", -1024, bufferSize);
  }

  @Test
  public void testZeroIOFileBufferSize() throws Exception {
    Configuration conf = new Configuration(false);
    // When zero is provided, Configuration returns that exact value
    String invalidConf =
        "<?xml version=\"1.0\"?>\n" +
        "<configuration>\n" +
        "  <property>\n" +
        "    <name>io.file.buffer.size</name>\n" +
        "    <value>0</value>\n" +
        "  </property>\n" +
        "</configuration>";
    try (InputStream is = new ByteArrayInputStream(invalidConf.getBytes(StandardCharsets.UTF_8))) {
      conf.addResource(is);
    }
    int bufferSize = conf.getInt(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
                                 CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);
    // Expect the value we set to be returned
    assertEquals("io.file.buffer.size must be 0", 0, bufferSize);
  }

  @Test
  public void testNonMultipleIOFileBufferSize() throws Exception {
    Configuration conf = new Configuration(false);
    // When non-multiple is provided, Configuration returns that exact value
    String invalidConf =
        "<?xml version=\"1.0\"?>\n" +
        "<configuration>\n" +
        "  <property>\n" +
        "    <name>io.file.buffer.size</name>\n" +
        "    <value>5000</value>\n" +
        "  </property>\n" +
        "</configuration>";
    try (InputStream is = new ByteArrayInputStream(invalidConf.getBytes(StandardCharsets.UTF_8))) {
      conf.addResource(is);
    }
    int bufferSize = conf.getInt(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
                                 CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);
    // Expect the value we set to be returned
    assertEquals("io.file.buffer.size must be 5000", 5000, bufferSize);
  }
}