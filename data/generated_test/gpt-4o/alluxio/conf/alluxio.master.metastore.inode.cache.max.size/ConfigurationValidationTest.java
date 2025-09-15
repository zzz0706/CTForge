package alluxio.master.metastore;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class ConfigurationValidationTest {

  @Test
  public void validateMasterMetastoreInodeCacheMaxSize() {
    // Test step: Retrieve the configuration value through Alluxio 2.1.0 API
    int maxSize = ServerConfiguration.global().getInt(PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE);

    // Validate the configuration satisfies constraints (must be positive)
    Assert.assertTrue(String.format("Configuration '%s' must have a positive value, but got %d",
        PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE.getName(), maxSize),
        maxSize > 0);
  }

  @Test
  public void validateCacheWaterMarkRatios() {
    // Retrieve the high and low water mark ratios using the correct API
    float highWaterMarkRatio = ServerConfiguration.global()
        .getFloat(PropertyKey.MASTER_METASTORE_INODE_CACHE_HIGH_WATER_MARK_RATIO);
    float lowWaterMarkRatio = ServerConfiguration.global()
        .getFloat(PropertyKey.MASTER_METASTORE_INODE_CACHE_LOW_WATER_MARK_RATIO);

    // The low water mark should not exceed the high water mark
    Assert.assertTrue(String.format("Low water mark ratio (%s=%f) must not exceed high water mark ratio (%s=%f",
        PropertyKey.MASTER_METASTORE_INODE_CACHE_LOW_WATER_MARK_RATIO.getName(), lowWaterMarkRatio,
        PropertyKey.MASTER_METASTORE_INODE_CACHE_HIGH_WATER_MARK_RATIO.getName(), highWaterMarkRatio),
        lowWaterMarkRatio <= highWaterMarkRatio);
  }

  @Test
  public void validateEvictBatchSize() {
    // Retrieve evict batch size using Alluxio's API
    int evictBatchSize = ServerConfiguration.global()
        .getInt(PropertyKey.MASTER_METASTORE_INODE_CACHE_EVICT_BATCH_SIZE);

    // Ensure evictBatchSize is positive
    Assert.assertTrue(String.format("Configuration '%s' must have a positive value, but got %d",
        PropertyKey.MASTER_METASTORE_INODE_CACHE_EVICT_BATCH_SIZE.getName(), evictBatchSize),
        evictBatchSize > 0);
  }
}