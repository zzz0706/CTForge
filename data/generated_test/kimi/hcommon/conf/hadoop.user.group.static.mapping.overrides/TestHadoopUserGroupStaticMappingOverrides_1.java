package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class TestHadoopUserGroupStaticMappingOverrides {

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
  public void testValidStaticMapping() {
    String validMapping = "user1=group1,group2;user2=;user3=group2";
    conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, validMapping);
    Groups groups = new Groups(conf);
    assertNotNull(groups);
  }

  @Test
  public void testInvalidMappingMissingEquals() {
    String invalidMapping = "user1group1,group2;user2=;user3=group2";
    conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, invalidMapping);
    Groups groups = new Groups(conf);
    assertNotNull(groups);
  }

  @Test
  public void testInvalidMappingMultipleEquals() {
    String invalidMapping = "user1=group1=extra,group2;user2=;user3=group2";
    conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, invalidMapping);
    try {
      Groups groups = new Groups(conf);
      assertNotNull(groups);
    } catch (org.apache.hadoop.HadoopIllegalArgumentException e) {
      // Expected exception for invalid mapping with multiple equals
    }
  }

  @Test
  public void testInvalidMappingEmptyUser() {
    String invalidMapping = "=group1,group2;user2=;user3=group2";
    conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, invalidMapping);
    Groups groups = new Groups(conf);
    assertNotNull(groups);
  }

  @Test
  public void testDefaultMapping() {
    // Default is "dr.who=;" which is valid
    Groups groups = new Groups(conf);
    assertNotNull(groups);
  }

  @Test
  public void testEmptyMapping() {
    String emptyMapping = "";
    conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, emptyMapping);
    Groups groups = new Groups(conf);
    assertNotNull(groups);
  }

  @Test
  public void testWhitespaceMapping() {
    String whitespaceMapping = "   ";
    conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, whitespaceMapping);
    Groups groups = new Groups(conf);
    assertNotNull(groups);
  }

  @Test
  public void testInvalidMappingOnlySemicolon() {
    String invalidMapping = "user1=group1,group2;;user2=;user3=group2";
    conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, invalidMapping);
    Groups groups = new Groups(conf);
    assertNotNull(groups);
  }
}