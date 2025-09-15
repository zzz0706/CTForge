package org.apache.hadoop.hbase.replication;

import static org.junit.Assert.assertThrows;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.client.Admin;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
//HBASE-19783
public class TestClusterBlocked {

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void changeClusterKeyToBlank_shouldBeRejected_afterPatch() throws Exception {
    Admin admin = UTIL.getAdmin();

    ReplicationPeerConfig init = ReplicationPeerConfig.newBuilder()
        .setClusterKey("127.0.0.1:1:/hbase")
        .build();
    String peerId = "p-change-ckey";
    admin.addReplicationPeer(peerId, init);

    ReplicationPeerConfig updated = ReplicationPeerConfig.newBuilder(init)
        .setClusterKey("") 
        .build();

    assertThrows(DoNotRetryIOException.class,
        () -> admin.updateReplicationPeerConfig(peerId, updated));
  }
}
