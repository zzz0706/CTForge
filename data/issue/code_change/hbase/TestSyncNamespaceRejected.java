package org.apache.hadoop.hbase.replication;

import static org.junit.Assert.assertThrows;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
//HBASE-19935
public class TestSyncNamespaceRejected {

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(1);
    Admin admin = UTIL.getAdmin();
    admin.createNamespace(NamespaceDescriptor.create("ns1").build());
    admin.createTable(
        TableDescriptorBuilder.newBuilder(TableName.valueOf("ns1:t1"))
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf"))
            .build());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void syncReplication_withNamespaceMapping_shouldBeRejected() throws Exception {
    Admin admin = UTIL.getAdmin();

    Path walDir = new Path(UTIL.getDataTestDir("remote-wal").toString());

    ReplicationPeerConfig rpc = ReplicationPeerConfig.newBuilder()
        .setClusterKey("127.0.0.1:1:/hbase")
        .setRemoteWALDir(walDir.toString())           
        .setReplicateAllUserTables(false)
        .setNamespaces(new HashSet<>(Collections.singletonList("ns1"))) 
        .setTableCFsMap(new HashMap<>())            
        .build();

    assertThrows(DoNotRetryIOException.class,
        () -> admin.addReplicationPeer("p-sync-ns", rpc));
  }
}
