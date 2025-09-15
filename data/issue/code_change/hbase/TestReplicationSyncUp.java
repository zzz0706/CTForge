package org.apache.hadoop.hbase.replication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Test;

//Hbase-20586
public class TestReplicationSyncUp {

    @Test
    public void testAuthenticationConfigIsHandledCaseInsensitively() {

        Configuration conf = HBaseConfiguration.create();
        Configuration peerConf = HBaseConfiguration.create();

        String mainKeytab = "/path/to/main/user.keytab";
        String peerKeytab = "/path/to/peer/user.keytab";

      
        peerConf.set("hbase.security.authentication", "Kerberos");
        peerConf.set("hbase.client.keytab.file", peerKeytab);

        conf.set("hbase.client.keytab.file", mainKeytab);

        String resultKeytab = ReplicationSyncUp.getPeerClusterKeytab(peerConf, conf);

        assertNotNull(
            "The keytab should not be null. A null result indicates the check for " +
            "Kerberos authentication was likely case-sensitive, which is incorrect. " +
            "This check should be case-insensitive.",
            resultKeytab);

        assertEquals(
            "The returned keytab should be the one defined in the peer's configuration.",
            peerKeytab,
            resultKeytab);
    }
}