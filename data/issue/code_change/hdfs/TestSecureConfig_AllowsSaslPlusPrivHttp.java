package org.apache.hadoop.hdfs.server.datanode;

import static org.mockito.Mockito.*;

import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.common.HttpConfig;
import org.junit.Test;


public class TestSecureConfig_AllowsSaslPlusPrivHttp {

  @Test
  public void saslPlusPrivilegedHttp_shouldPass_afterFix() throws Exception {

    Configuration conf = new HdfsConfiguration();
    conf.setEnum(DFSConfigKeys.DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTP_ONLY);

    conf.set(DFSConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY, "privacy");

    DNConf dnConf = new DNConf(conf);

    SecureResources resources = mock(SecureResources.class);
    when(resources.isHttpPortPrivileged()).thenReturn(true);
    when(resources.isRpcPortPrivileged()).thenReturn(false);
    when(resources.isSaslEnabled()).thenReturn(true);

    Method m = DataNode.class.getDeclaredMethod(
        "checkSecureConfig", DNConf.class, Configuration.class, SecureResources.class);
    m.setAccessible(true);

    m.invoke(null, dnConf, conf, resources);
  }
}
