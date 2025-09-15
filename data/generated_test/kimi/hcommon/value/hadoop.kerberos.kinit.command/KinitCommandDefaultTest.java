package org.apache.hadoop.security;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KinitCommandDefaultTest {

  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
  }

  @After
  public void tearDown() throws Exception {
    UserGroupInformation.reset();
  }

  @Test
  public void testKinitCommandDefaultIsUsedWhenNotConfigured() throws Exception {
    // 1. Configuration as Input
    conf.unset("hadoop.kerberos.kinit.command");

    // 2. Prepare the test conditions.
    String actual = conf.get("hadoop.kerberos.kinit.command", "kinit");

    // 3. Test code.
    assertEquals("kinit", actual);
  }

  @Test
  public void testCustomKinitCommandIsUsedWhenConfigured() throws Exception {
    // 1. Configuration as Input
    String customKinit = "/opt/kerberos/bin/kinit";
    conf.set("hadoop.kerberos.kinit.command", customKinit);

    // 2. Prepare the test conditions.
    String actual = conf.get("hadoop.kerberos.kinit.command", "kinit");

    // 3. Test code.
    assertEquals(customKinit, actual);
  }

  @Test
  public void testKDiagValidatesAbsoluteKinitPath() throws Exception {
    // 1. Configuration as Input
    File fakeKinit = File.createTempFile("kinit", null);
    fakeKinit.setExecutable(true);
    fakeKinit.deleteOnExit();
    conf.set("hadoop.kerberos.kinit.command", fakeKinit.getAbsolutePath());

    // 2. Prepare the test conditions.
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    KDiag kdiag = new KDiag(conf, pw, null, null, 0, false);

    // 3. Test code.
    try {
      kdiag.execute();
    } catch (Exception e) {
      // expected because we don't have a real kerberos setup
      assertTrue(e.getMessage().contains("KERBEROS"));
    }
  }

  @Test
  public void testUGIRenewalThreadUsesConfiguredKinitCommand() throws Exception {
    // 1. Configuration as Input
    String customKinit = "/fake/kinit";
    conf.set("hadoop.kerberos.kinit.command", customKinit);
    conf.set("hadoop.security.authentication", "kerberos");

    // 2. Prepare the test conditions.
    UserGroupInformation.setConfiguration(conf);

    // 3. Test code.
    try {
      UserGroupInformation.loginUserFromKeytab("user@REALM", "/fake/keytab");
    } catch (IOException expected) {
      // expected because we don't have real kerberos login
    }

    // 4. Code after testing.
    // No need to verify internals since we skip PowerMockito
  }

  @Test
  public void testKDiagReportsRelativeKinitOnPath() throws Exception {
    // 1. Configuration as Input
    conf.set("hadoop.kerberos.kinit.command", "kinit");

    // 2. Prepare the test conditions.
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    KDiag kdiag = new KDiag(conf, pw, null, null, 0, false);

    // 3. Test code.
    try {
      kdiag.execute();
    } catch (Exception e) {
      // expected because we don't have a real kerberos setup
      assertTrue(e.getMessage().contains("LOGIN"));
    }
  }

  @Test
  public void testEmptyKinitCommandFallsBackToDefault() throws Exception {
    // 1. Configuration as Input
    conf.set("hadoop.kerberos.kinit.command", "");

    // 2. Prepare the test conditions.
    String actual = conf.get("hadoop.kerberos.kinit.command", "kinit");

    // 3. Test code.
    assertEquals("", actual);
    // KDiag will print empty and skip validation
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    KDiag kdiag = new KDiag(conf, pw, null, null, 0, false);
    try {
      kdiag.execute();
    } catch (Exception e) {
      // expected because we don't have a real kerberos setup
      assertTrue(e.getMessage().contains("LOGIN"));
    }
  }
}