package org.apache.hadoop.hbase.master;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.normalizer.RegionNormalizer;
import org.apache.hadoop.hbase.master.normalizer.RegionNormalizerFactory;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, SmallTests.class })
public class HMasterNormalizerConfigTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(HMasterNormalizerConfigTest.class);

  @Test
  public void testNormalizerReceivesCorrectServicesAfterInstantiation() throws Exception {
    // 1. Instantiate Configuration
    Configuration conf = new Configuration();

    // 2. Dynamic expected value calculation
    Class<?> expectedNormalizerClass = conf.getClass(
        HConstants.HBASE_MASTER_NORMALIZER_CLASS,
        org.apache.hadoop.hbase.master.normalizer.SimpleRegionNormalizer.class,
        RegionNormalizer.class);

    // 3. Mock/stub external dependencies
    RegionNormalizer normalizerMock = mock(RegionNormalizer.class);
    HMaster master = mock(HMaster.class);
    MasterRpcServices rpcServices = mock(MasterRpcServices.class);

    // Stub minimal master initialization to avoid NPE
    when(master.getConfiguration()).thenReturn(conf);

    // 4. Invoke the method under test
    RegionNormalizer actualNormalizer = RegionNormalizerFactory.getRegionNormalizer(conf);
    actualNormalizer.setMasterServices(master);
    actualNormalizer.setMasterRpcServices(rpcServices);

    // 5. Assertions and verification
    assertEquals(expectedNormalizerClass, actualNormalizer.getClass());
    verify(normalizerMock, times(0)).setMasterServices(master);
    verify(normalizerMock, times(0)).setMasterRpcServices(rpcServices);
  }
}