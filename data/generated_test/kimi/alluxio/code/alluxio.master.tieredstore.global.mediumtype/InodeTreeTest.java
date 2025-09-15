package alluxio.master.file.meta;

import alluxio.AlluxioURI;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.MountPOptions;
import alluxio.master.block.ContainerIdGenerable;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.master.metastore.InodeStore;
import alluxio.master.metastore.heap.HeapInodeStore;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.local.LocalUnderFileSystemFactory;
import alluxio.util.IdUtils;

import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Collections;

import static org.junit.Assert.assertFalse;

public class InodeTreeTest {

  private InodeTree mInodeTree;

  @Before
  public void before() throws Exception {
    ServerConfiguration.reset();
    ServerConfiguration.set(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, "mem:///");

    InodeStore inodeStore = new HeapInodeStore();
    ContainerIdGenerable containerIdGenerator = new ContainerIdGenerator();
    InodeDirectoryIdGenerator inodeDirectoryIdGenerator = new InodeDirectoryIdGenerator(containerIdGenerator);

    UfsManager ufsManager = new UfsManager() {
      @Override
      public UfsClient get(long mountId) {
        return null;
      }

      @Override
      public UfsClient getRoot() {
        UnderFileSystemConfiguration ufsConf = UnderFileSystemConfiguration.defaults(ServerConfiguration.global());
        return new UfsClient(() -> new LocalUnderFileSystemFactory().create("mem:///", ufsConf), new AlluxioURI("mem:///"));
      }

      @Override
      public void addMount(long mountId, AlluxioURI ufsUri, UnderFileSystemConfiguration ufsConf) {}

      @Override
      public void removeMount(long mountId) {}

      @Override
      public void close() {}
    };

    MountTable mountTable = new MountTable(ufsManager,
        new MountInfo(new AlluxioURI("mem:///"),
                      new AlluxioURI("mem:///"),
                      IdUtils.ROOT_MOUNT_ID,
                      MountPOptions.getDefaultInstance()));
    InodeLockManager inodeLockManager = new InodeLockManager();

    mInodeTree = new InodeTree(
        inodeStore,
        containerIdGenerator,
        inodeDirectoryIdGenerator,
        mountTable,
        inodeLockManager
    );
  }

  @After
  public void after() {
    ServerConfiguration.reset();
  }

  @Test
  public void caseInsensitiveMediumNames() throws Exception {
    // 1. Use the alluxio2.1.0 API to read the actual value
    String defaultMediums = ServerConfiguration.get(PropertyKey.MASTER_TIERED_STORE_GLOBAL_MEDIUMTYPE);

    // 2. Prepare the test conditions
    Method checkPinningValidityMethod = InodeTree.class.getDeclaredMethod("checkPinningValidity", java.util.Set.class);
    checkPinningValidityMethod.setAccessible(true);

    // 3. Test code
    boolean actual = (boolean) checkPinningValidityMethod.invoke(mInodeTree, Sets.newHashSet("mem"));

    // 4. Assertions
    assertFalse("Lower-case 'mem' should be rejected when only 'MEM' is allowed", actual);
  }

  // Simple implementations for final classes that cannot be mocked
  private static class ContainerIdGenerator implements ContainerIdGenerable {
    private long mNextContainerId = 1;

    @Override
    public long getNewContainerId() {
      return mNextContainerId++;
    }
  }
}