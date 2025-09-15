package org.apache.hadoop.hdfs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestTrustedChannelResolverConfig {

  private Configuration conf;

  @Before
  public void setUp() {
    conf = new Configuration();
  }

  @After
  public void tearDown() {
    conf = null;
  }

  @Test
  public void testDefaultResolverClass() {
    // 1. No explicit value set; should load default
    // 2. Prepare test conditions: leave conf untouched
    // 3. Test code
    TrustedChannelResolver resolver = TrustedChannelResolver.getInstance(conf);
    assertNotNull("Default resolver must be non-null", resolver);
    assertEquals("Default resolver class must be TrustedChannelResolver",
                 TrustedChannelResolver.class, resolver.getClass());
  }

  @Test
  public void testValidCustomResolverClass() {
    // 1. Set a valid subclass of TrustedChannelResolver
    // 2. Prepare test conditions
    conf.setClass(HdfsClientConfigKeys.DFS_TRUSTEDCHANNEL_RESOLVER_CLASS,
                  DummyTrustedChannelResolver.class, TrustedChannelResolver.class);
    // 3. Test code
    TrustedChannelResolver resolver = TrustedChannelResolver.getInstance(conf);
    assertNotNull("Custom resolver must be non-null", resolver);
    assertEquals("Custom resolver class must be DummyTrustedChannelResolver",
                 DummyTrustedChannelResolver.class, resolver.getClass());
  }

  @Test(expected = RuntimeException.class)
  public void testInvalidResolverClassName() {
    // 1. Set a non-existent class name
    // 2. Prepare test conditions
    conf.set(HdfsClientConfigKeys.DFS_TRUSTEDCHANNEL_RESOLVER_CLASS,
             "com.example.NonExistentResolver");
    // 3. Test code – should throw RuntimeException via ReflectionUtils
    TrustedChannelResolver.getInstance(conf);
  }

  @Test(expected = RuntimeException.class)
  public void testNonAssignableResolverClass() {
    // 1. Set a class that does not extend TrustedChannelResolver
    // 2. Prepare test conditions
    conf.setClass(HdfsClientConfigKeys.DFS_TRUSTEDCHANNEL_RESOLVER_CLASS,
                  String.class, TrustedChannelResolver.class);
    // 3. Test code – should throw RuntimeException via ReflectionUtils
    TrustedChannelResolver.getInstance(conf);
  }

  // Dummy implementation for testing valid custom resolver
  public static class DummyTrustedChannelResolver extends TrustedChannelResolver {
  }
}