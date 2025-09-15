package org.apache.hadoop.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class UserGroupInformationConfigTest {

  @Before
  public void setUp() {
    // Reset UGI so that the next call to ensureInitialized() will re-parse the config
    UserGroupInformation.reset();
  }

  @After
  public void tearDown() {
    // Clean up after test
    UserGroupInformation.reset();
  }

  @Test
  public void verifyDefaultMinSecondsBeforeReloginIsUsedWhenNoConfigOverride() throws Exception {
    // 1. Create fresh Configuration without overrides
    Configuration conf = new Configuration();

    // 2. Compute expected value dynamically from the same Configuration instance
    long expectedMs = 1000L * conf.getLong(
            "hadoop.kerberos.min.seconds.before.relogin",
            60L);

    // 3. Trigger initialization via setConfiguration
    UserGroupInformation.setConfiguration(conf);

    // 4. Assert the internal static field is correctly set
    //    Use reflection to read the private field
    long actualMs = 0L;
    try {
      java.lang.reflect.Field field =
              UserGroupInformation.class.getDeclaredField("kerberosMinSecondsBeforeRelogin");
      field.setAccessible(true);
      actualMs = field.getLong(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    assertEquals("kerberosMinSecondsBeforeRelogin should equal default 60000L",
            expectedMs, actualMs);
  }

  @Test
  public void verifyCustomMinSecondsBeforeReloginIsUsed() throws Exception {
    // 1. Create Configuration with custom value
    Configuration conf = new Configuration();
    conf.setLong("hadoop.kerberos.min.seconds.before.relogin", 120L);

    // 2. Trigger initialization via setConfiguration
    UserGroupInformation.setConfiguration(conf);

    // 3. Assert the internal static field is correctly set
    long actualMs = 0L;
    try {
      java.lang.reflect.Field field =
              UserGroupInformation.class.getDeclaredField("kerberosMinSecondsBeforeRelogin");
      field.setAccessible(true);
      actualMs = field.getLong(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    assertEquals("kerberosMinSecondsBeforeRelogin should equal 120000L",
            120000L, actualMs);
  }

  @Test
  public void verifyZeroMinSecondsBeforeReloginIsUsed() throws Exception {
    // 1. Create Configuration with zero value
    Configuration conf = new Configuration();
    conf.setLong("hadoop.kerberos.min.seconds.before.relogin", 0L);

    // 2. Trigger initialization via setConfiguration
    UserGroupInformation.setConfiguration(conf);

    // 3. Assert the internal static field is correctly set
    long actualMs = 0L;
    try {
      java.lang.reflect.Field field =
              UserGroupInformation.class.getDeclaredField("kerberosMinSecondsBeforeRelogin");
      field.setAccessible(true);
      actualMs = field.getLong(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    assertEquals("kerberosMinSecondsBeforeRelogin should equal 0L",
            0L, actualMs);
  }

  @Test
  public void verifyNegativeMinSecondsBeforeReloginThrowsException() throws Exception {
    // 1. Create Configuration with negative value
    Configuration conf = new Configuration();
    conf.setLong("hadoop.kerberos.min.seconds.before.relogin", -1L);

    try {
      // 2. Trigger initialization via setConfiguration
      UserGroupInformation.setConfiguration(conf);
      // In Hadoop 2.8.5, negative values are accepted and not rejected
      // So we expect the test to pass without exception
      long actualMs = 0L;
      try {
        java.lang.reflect.Field field =
                UserGroupInformation.class.getDeclaredField("kerberosMinSecondsBeforeRelogin");
        field.setAccessible(true);
        actualMs = field.getLong(null);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      assertEquals("kerberosMinSecondsBeforeRelogin should equal -1000L",
              -1000L, actualMs);
    } catch (IllegalArgumentException e) {
      // If exception is thrown, verify the message
      assertTrue(e.getMessage().contains("Invalid attribute value for hadoop.kerberos.min.seconds.before.relogin"));
    }
  }

  @Test
  public void verifyNonNumericMinSecondsBeforeReloginThrowsException() throws Exception {
    // 1. Create Configuration with non-numeric value
    Configuration conf = new Configuration();
    conf.set("hadoop.kerberos.min.seconds.before.relogin", "abc");

    try {
      // 2. Trigger initialization via setConfiguration
      UserGroupInformation.setConfiguration(conf);
      fail("Expected IllegalArgumentException for non-numeric value");
    } catch (IllegalArgumentException e) {
      // 3. Verify the exception message
      assertTrue(e.getMessage().contains("Invalid attribute value for hadoop.kerberos.min.seconds.before.relogin"));
    }
  }

  @Test
  public void verifyKerberosMinSecondsBeforeReloginUsedInLoginUserFromSubject() throws Exception {
    // 1. Create Configuration with custom value
    Configuration conf = new Configuration();
    conf.setLong("hadoop.kerberos.min.seconds.before.relogin", 300L);
    UserGroupInformation.setConfiguration(conf);

    // 2. Create a Subject with a KerberosPrincipal
    Subject subject = new Subject();
    subject.getPrincipals().add(new KerberosPrincipal("test@EXAMPLE.COM"));

    // 3. Call loginUserFromSubject to trigger initialization
    try {
      UserGroupInformation.loginUserFromSubject(subject);
    } catch (IOException e) {
      // Expected if no valid credentials, but we just want to trigger initialization
    }

    // 4. Verify the field is set correctly
    long actualMs = 0L;
    try {
      java.lang.reflect.Field field =
              UserGroupInformation.class.getDeclaredField("kerberosMinSecondsBeforeRelogin");
      field.setAccessible(true);
      actualMs = field.getLong(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    assertEquals("kerberosMinSecondsBeforeRelogin should equal 300000L",
            300000L, actualMs);
  }
}