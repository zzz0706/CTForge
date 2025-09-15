package org.apache.hadoop.hdfs.server.namenode;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.*;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSDirXAttrOp;

import java.util.EnumSet;
import java.util.List;
import com.google.common.collect.Lists;

public class FSDirXAttrOpTest {

    @Mock
    private FSDirectory fsDirectoryMock;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void withinXAttrLimitReturnsMergedXAttrs() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // Mock the method getInodeXAttrsLimit to simulate the XAttr limit retrieved dynamically from the HDFS configuration.
        int maxXAttrsPerInode = 32; // MAX_XATTRS_PER_INODE is removed in HdfsConstants in 2.8.5, use hardcoded value relevant to HDFS 2.8.5 default.
        when(fsDirectoryMock.getInodeXAttrsLimit()).thenReturn(maxXAttrsPerInode);

        // 2. Prepare the test conditions: Define the current XAttrs and the ones to add.
        List<XAttr> existingXAttrs = Lists.newArrayList(
                new XAttr.Builder().setNameSpace(XAttr.NameSpace.USER).setName("attr1").build(),
                new XAttr.Builder().setNameSpace(XAttr.NameSpace.USER).setName("attr2").build(),
                new XAttr.Builder().setNameSpace(XAttr.NameSpace.USER).setName("attr3").build(),
                new XAttr.Builder().setNameSpace(XAttr.NameSpace.USER).setName("attr4").build()
        );
        List<XAttr> toSet = Lists.newArrayList(
                new XAttr.Builder().setNameSpace(XAttr.NameSpace.USER).setName("attr5").build()
        );
        EnumSet<XAttrSetFlag> flags = EnumSet.of(XAttrSetFlag.CREATE);

        // 3. Test code: Call the FSDirXAttrOp setINodeXAttrs function with the prepared conditions.
        List<XAttr> result = FSDirXAttrOp.setINodeXAttrs(fsDirectoryMock, existingXAttrs, toSet, flags);

        // 4. Code after testing: Validate the result to ensure that the XAttrs merge correctly within the limit.
        assertEquals(5, result.size());
    }
}