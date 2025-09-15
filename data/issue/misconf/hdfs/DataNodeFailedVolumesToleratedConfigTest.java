package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.Assert;
import org.junit.Test;
//hdfs-10279
public class DataNodeFailedVolumesToleratedConfigTest {

    private static final String KEY = DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY;
    private static final int DEFAULT = DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_DEFAULT;

    private void validate(Configuration conf) {
        int v = conf.getInt(KEY, DEFAULT);
        if (v < 0) {
            throw new IllegalArgumentException(
                String.format("dfs.datanode.failed.volumes.tolerated  %s=%d must >= 0", KEY, v)
            );
        }
    }

    @Test
    public void testFailedVolumesToleratedValidation() {
        Configuration conf = new HdfsConfiguration();
        validate(conf);
    }
}
