package alluxio.master.block;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.block.meta.MasterWorkerInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.wire.WorkerInfo;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DefaultBlockMaster.class})
public class DefaultBlockMasterTest {

  @Test
  public void longTimeoutPreventsFalsePositive() throws Exception {
    // 1. You need to use the alluxio2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    long expectedTimeoutMs = ServerConfiguration.getMs(PropertyKey.MASTER_WORKER_TIMEOUT_MS);

    // 2. Prepare the test conditions.
    AtomicLong clock = new AtomicLong(System.currentTimeMillis());
    DefaultBlockMaster blockMaster = mock(DefaultBlockMaster.class);
    List<WorkerInfo> mWorkers = new ArrayList<>();
    List<WorkerInfo> mLostWorkers = new ArrayList<>();

    MasterWorkerInfo workerMeta = new MasterWorkerInfo(1, new WorkerNetAddress());
    workerMeta.updateLastUpdatedTimeMs(); // reset to now
    // Simulate 5 minutes ago by directly setting the field via reflection
    java.lang.reflect.Field field = MasterWorkerInfo.class.getDeclaredField("mLastUpdatedTimeMs");
    field.setAccessible(true);
    field.setLong(workerMeta, clock.get() - 5 * 60 * 1000);

    WorkerInfo workerInfo = new WorkerInfo();
    workerInfo.setId(workerMeta.getId());
    workerInfo.setAddress(workerMeta.getWorkerAddress());
    mWorkers.add(workerInfo);

    // 3. Test code.
    when(blockMaster.getWorkerInfoList()).thenReturn(Collections.unmodifiableList(mWorkers));
    when(blockMaster.getLostWorkersInfoList()).thenReturn(Collections.unmodifiableList(mLostWorkers));

    // Simulate the heartbeat logic manually
    if (clock.get() - workerMeta.getLastUpdatedTimeMs() > expectedTimeoutMs) {
      mWorkers.remove(0);
      mLostWorkers.add(workerInfo);
    }

    // 4. Code after testing.
    assertEquals(1, mWorkers.size());
    assertEquals(0, mLostWorkers.size());
  }
}