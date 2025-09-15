package org.apache.hadoop.hdfs.qjournal.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.net.NetUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

//HDFS-12407
public class JournalNodeHttpServerShutdownTest {

    @Test
    public void testStopAfterFailedStart() throws IOException {

        Configuration conf = new HdfsConfiguration();

        String addrStr = conf.get(
                DFSConfigKeys.DFS_JOURNALNODE_HTTP_ADDRESS_KEY,
                DFSConfigKeys.DFS_JOURNALNODE_HTTP_ADDRESS_DEFAULT);
        InetSocketAddress bindAddress = NetUtils.createSocketAddr(addrStr);
        int port = bindAddress.getPort();

        JournalNode fakeJn = new JournalNode();
        JournalNodeHttpServer server = new JournalNodeHttpServer(conf, fakeJn);

        try (ServerSocket ss = new ServerSocket(port)) {
            boolean startFailed = false;
            try {
                server.start();
            } catch (IOException e) {
                startFailed = true;
            }
            Assert.assertTrue(
                    "Expected an IOException to be thrown when the port is occupied.",
                    startFailed);
            server.stop();
        }
    }
}
