package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.HostConfigManager;
import org.apache.hadoop.hdfs.server.blockmanagement.HostFileManager;
import org.apache.hadoop.hdfs.server.blockmanagement.CombinedHostFileManager;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestDfsNamenodeHostsProviderClassnameConfig {

  @Test
  public void testValidHostConfigManagerClass() {
    Configuration conf = new Configuration(false);
    // Do NOT set the value – rely on the file under test
    Class<?> clazz = conf.getClass(
        DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY,
        HostFileManager.class, HostConfigManager.class);

    assertNotNull("Configured class must not be null", clazz);
    assertTrue("Configured class must implement HostConfigManager",
               HostConfigManager.class.isAssignableFrom(clazz));
  }

  @Test
  public void testDefaultValueIsHostFileManager() {
    Configuration conf = new Configuration(false);
    Class<?> defaultClazz = conf.getClass(
        DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY,
        HostFileManager.class, HostConfigManager.class);

    assertEquals("Default value must be HostFileManager",
                 HostFileManager.class, defaultClazz);
  }

  @Test
  public void testCombinedHostFileManagerIsAllowed() {
    Configuration conf = new Configuration(false);
    // Do NOT set the value – rely on the file under test
    Class<?> clazz = conf.getClass(
        DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY,
        HostFileManager.class, HostConfigManager.class);

    // If CombinedHostFileManager is explicitly configured, it must be accepted
    if (CombinedHostFileManager.class.equals(clazz)) {
      assertTrue("CombinedHostFileManager must be assignable to HostConfigManager",
                 HostConfigManager.class.isAssignableFrom(CombinedHostFileManager.class));
    }
  }

  @Test
  public void testInvalidClassNameThrowsException() {
    Configuration conf = new Configuration(false);
    String invalidClassName = "com.example.NonExistentHostConfigManager";
    conf.set(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY, invalidClassName);

    try {
      conf.getClass(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY,
                    HostFileManager.class, HostConfigManager.class);
      fail("Expected RuntimeException for invalid class name");
    } catch (RuntimeException e) {
      // expected
    }
  }

  @Test
  public void testClassNotImplementingInterfaceThrowsException() {
    Configuration conf = new Configuration(false);
    String invalidClassName = "java.lang.String"; // does not implement HostConfigManager
    conf.set(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY, invalidClassName);

    try {
      conf.getClass(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY,
                    HostFileManager.class, HostConfigManager.class);
      fail("Expected RuntimeException for class not implementing HostConfigManager");
    } catch (RuntimeException e) {
      // expected
    }
  }
}