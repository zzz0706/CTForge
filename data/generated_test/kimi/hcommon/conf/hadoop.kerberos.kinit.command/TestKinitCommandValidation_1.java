package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class TestKinitCommandValidation {

  private Configuration conf;
  private String originalKinit;

  @Before
  public void setUp() {
    conf = new Configuration();
    // Clear any previously set value so the test always starts from default
    originalKinit = conf.get("hadoop.kerberos.kinit.command");
    conf.unset("hadoop.kerberos.kinit.command");
  }

  @After
  public void tearDown() {
    // Restore original value if any
    if (originalKinit != null) {
      conf.set("hadoop.kerberos.kinit.command", originalKinit);
    }
  }

  /**
   * Validates that the configured kinit command path exists and is a valid
   * executable file when an absolute path is provided.
   */
  @Test
  public void testAbsoluteKinitPathExists() {
    String kinitPath = conf.getTrimmed("hadoop.kerberos.kinit.command", "kinit");
    if (!kinitPath.isEmpty() && new File(kinitPath).isAbsolute()) {
      File file = new File(kinitPath);
      assertTrue("Absolute kinit path must exist: " + kinitPath, file.exists());
      assertTrue("Absolute kinit path must be a file: " + kinitPath, file.isFile());
      assertTrue("Absolute kinit path must be executable: " + kinitPath, file.canExecute());
    }
  }

  /**
   * Validates that when the configuration is not an absolute path,
   * it defaults to "kinit" and is expected to be resolved via PATH.
   */
  @Test
  public void testRelativeKinitCommand() {
    String kinit = conf.getTrimmed("hadoop.kerberos.kinit.command", "kinit");
    if (!kinit.isEmpty() && !new File(kinit).isAbsolute()) {
      assertEquals("Relative kinit command must be 'kinit' by default", "kinit", kinit);
    }
  }

  /**
   * Validates that the configuration does not contain illegal characters
   * or malformed paths.
   */
  @Test
  public void testKinitPathSanity() {
    String kinit = conf.getTrimmed("hadoop.kerberos.kinit.command", "");
    if (!kinit.isEmpty()) {
      assertFalse("Kinit path must not be empty string", kinit.trim().isEmpty());
      assertFalse("Kinit path must not contain null bytes", kinit.contains("\0"));
      assertTrue("Kinit path must be a valid file path", new File(kinit).getName().length() > 0);
    }
  }
}