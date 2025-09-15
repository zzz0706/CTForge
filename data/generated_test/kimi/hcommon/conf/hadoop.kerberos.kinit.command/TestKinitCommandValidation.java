package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class TestKinitCommandValidation {

  @Test
  public void testKinitCommandPathValidation() {
    Configuration conf = new Configuration();
    String kinitCmd = conf.getTrimmed("hadoop.kerberos.kinit.command", "kinit");

    if (kinitCmd.isEmpty()) {
      // Empty is allowed; it falls back to "kinit" in the code
      return;
    }

    File kinitPath = new File(kinitCmd);
    if (kinitPath.isAbsolute()) {
      assertTrue("hadoop.kerberos.kinit.command points to a non-existent file: " + kinitCmd,
          kinitPath.exists());
      assertTrue("hadoop.kerberos.kinit.command points to a directory: " + kinitCmd,
          kinitPath.isFile());
      assertTrue("hadoop.kerberos.kinit.command points to an empty file: " + kinitCmd,
          kinitPath.length() > 0);
    } else {
      // Relative command; no validation possible beyond non-empty
      assertFalse("hadoop.kerberos.kinit.command is empty", kinitCmd.trim().isEmpty());
    }
  }
}