package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.*;

public class TestFsDefaultFSValidation {

  @Test
  public void testFsDefaultFSValidURI() throws Exception {
    Configuration conf = new Configuration(false);
    // rely on the actual configuration file loaded from classpath
    conf.addResource("core-site.xml");
    conf.addResource("hdfs-site.xml");

    String fsDefaultFS = conf.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
                                  CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT);

    assertNotNull("fs.defaultFS must be defined", fsDefaultFS);
    assertFalse("fs.defaultFS must not be empty", fsDefaultFS.trim().isEmpty());

    try {
      URI uri = new URI(fsDefaultFS);
      assertNotNull("fs.defaultFS must contain a scheme", uri.getScheme());
      assertFalse("fs.defaultFS scheme must not be empty", uri.getScheme().trim().isEmpty());
    } catch (URISyntaxException e) {
      fail("fs.defaultFS is not a valid URI: " + fsDefaultFS);
    }
  }
}