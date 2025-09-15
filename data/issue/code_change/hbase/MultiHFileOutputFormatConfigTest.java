package org.apache.hadoop.hbase.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.*;
//HBASE-16261
public class MultiHFileOutputFormatConfigTest {

  private static final String MRV2_KEY = "mapreduce.totalorderpartitioner.path";
  private static final String MRV1_KEY = "mapred.totalorderpartitioner.path";


  private static Map<TableName, HTableDescriptor> singleTableDesc(String table, String family) {
    TableName tn = TableName.valueOf(table);

    HTableDescriptor htd = new HTableDescriptor(tn);
    htd.addFamily(new org.apache.hadoop.hbase.HColumnDescriptor(family));
    Map<TableName, HTableDescriptor> map = new LinkedHashMap<>();
    map.put(tn, htd);
    return map;
  }

  private static String getPartitionFileFromConf(Configuration conf) {
    String p = conf.get(MRV2_KEY);
    if (p == null) p = conf.get(MRV1_KEY);
    return p;
  }

  @Test
  public void testPartitionFilePathIsCreatedAndUsesTmpDir() throws Exception {
    Configuration base = HBaseConfiguration.create();

    String userTmp = "/tmp/hbase-staging-" + UUID.randomUUID();
    base.set(HConstants.TEMPORARY_FS_DIRECTORY_KEY, userTmp);

    Job job = Job.getInstance(base);
    Map<TableName, HTableDescriptor> tds = singleTableDesc("t", "f");

    MultiHFileOutputFormat.configureIncrementalLoad(job, tds);

    String partPath = getPartitionFileFromConf(job.getConfiguration());
    assertNotNull("TotalOrderPartitioner path must be set by configureIncrementalLoad", partPath);

    assertTrue(
        "Partition file path should start with user tmp dir: " + partPath,
        partPath.startsWith(new Path(userTmp).toString())
    );
  }

  @Test
  public void testUserTmpDirIsRespected_NotFallingBackToDefault() throws Exception {
    Configuration base = HBaseConfiguration.create();

    Job jobDefault = Job.getInstance(new Configuration(base));
    Map<TableName, HTableDescriptor> tds = singleTableDesc("t2", "f2");
    MultiHFileOutputFormat.configureIncrementalLoad(jobDefault, tds);
    String defaultPartPath = getPartitionFileFromConf(jobDefault.getConfiguration());
    assertNotNull("Default partition file path should be set", defaultPartPath);

    String userTmp = "/tmp/hbase-staging-" + UUID.randomUUID();
    Configuration c2 = new Configuration(base);
    c2.set(HConstants.TEMPORARY_FS_DIRECTORY_KEY, userTmp);
    Job jobUser = Job.getInstance(c2);
    MultiHFileOutputFormat.configureIncrementalLoad(jobUser, tds);
    String userPartPath = getPartitionFileFromConf(jobUser.getConfiguration());
    assertNotNull(userPartPath);

    assertFalse(
        "User-specified tmp dir should change the partition file parent dir",
        new Path(defaultPartPath).getParent().toString().equals(new Path(userPartPath).getParent().toString())
    );
    assertTrue(
        "Partition file path should start with user tmp dir",
        userPartPath.startsWith(new Path(userTmp).toString())
    );
  }
}
