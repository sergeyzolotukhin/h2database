package org.h2.mvstore.page;

import org.h2.mvstore.CursorPos;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.FileStore;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.WriteBuffer;

import java.nio.ByteBuffer;

import static org.h2.engine.Constants.MEMORY_POINTER;

public class NonLeaf<K, V> extends Page<K, V> {
    /**
     * The child page references.
     */
    private PageReference<K, V>[] children;

    /**
     * The total entry count of this page and all children.
     */
    private long totalCount;

    public NonLeaf(MVMap<K, V> map) {
        super(map);
//            log.info("create page");
    }

    public NonLeaf(MVMap<K, V> map, NonLeaf<K, V> source, PageReference<K, V>[] children, long totalCount) {
        super(map, source);
        this.children = children;
        this.totalCount = totalCount;
//            log.info("create page");
    }

    public NonLeaf(MVMap<K, V> map, K[] keys, PageReference<K, V>[] children, long totalCount) {
        super(map, keys);
        this.children = children;
        this.totalCount = totalCount;
//            log.info("create page");
    }

    @Override
    public int getNodeType() {
        return DataUtils.PAGE_TYPE_NODE;
    }

    @Override
    public Page<K, V> copy(MVMap<K, V> map, boolean eraseChildrenRefs) {
        return eraseChildrenRefs ?
                new IncompleteNonLeaf<>(map, this) :
                new NonLeaf<>(map, this, children, totalCount);
    }

    @Override
    public Page<K, V> getChildPage(int index) {
        PageReference<K, V> ref = children[index];
        Page<K, V> page = ref.getPage();
        if (page == null) {
            page = map.readPage(ref.getPos());
            assert ref.getPos() == page.getPos();
            assert ref.count == page.getTotalCount();
        }
        return page;
    }

    @Override
    public long getChildPagePos(int index) {
        return children[index].getPos();
    }

    @Override
    public V getValue(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page<K, V> split(int at) {
        assert !isSaved();
        int b = getKeyCount() - at;
        K[] bKeys = splitKeys(at, b - 1);
        PageReference<K, V>[] aChildren = createRefStorage(at + 1);
        PageReference<K, V>[] bChildren = createRefStorage(b);
        System.arraycopy(children, 0, aChildren, 0, at + 1);
        System.arraycopy(children, at + 1, bChildren, 0, b);
        children = aChildren;

        long t = 0;
        for (PageReference<K, V> x : aChildren) {
            t += x.count;
        }
        totalCount = t;
        t = 0;
        for (PageReference<K, V> x : bChildren) {
            t += x.count;
        }
        Page<K, V> newPage = createNode(map, bKeys, bChildren, t, 0);
        if (isPersistent()) {
            recalculateMemory();
        }
        return newPage;
    }

    @Override
    public void expand(int keyCount, Object[] extraKeys, Object[] extraValues) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getTotalCount() {
        assert !isComplete() || totalCount == calculateTotalCount() :
                "Total count: " + totalCount + " != " + calculateTotalCount();
        return totalCount;
    }

    private long calculateTotalCount() {
        long check = 0;
        int keyCount = getKeyCount();
        for (int i = 0; i <= keyCount; i++) {
            check += children[i].count;
        }
        return check;
    }

    public void recalculateTotalCount() {
        totalCount = calculateTotalCount();
    }

    @Override
    public long getCounts(int index) {
        return children[index].count;
    }

    @Override
    public void setChild(int index, Page<K, V> c) {
        assert c != null;
        PageReference<K, V> child = children[index];
        if (c != child.getPage() || c.getPos() != child.getPos()) {
            totalCount += c.getTotalCount() - child.count;
            children = children.clone();
            children[index] = new PageReference<>(c);
        }
    }

    @Override
    public V setValue(int index, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void insertLeaf(int index, K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void insertNode(int index, K key, Page<K, V> childPage) {
        int childCount = getRawChildPageCount();
        insertKey(index, key);

        PageReference<K, V>[] newChildren = createRefStorage(childCount + 1);
        DataUtils.copyWithGap(children, newChildren, childCount, index);
        children = newChildren;
        children[index] = new PageReference<>(childPage);

        totalCount += childPage.getTotalCount();
        if (isPersistent()) {
            addMemory(MEMORY_POINTER + PAGE_MEMORY_CHILD);
        }
    }

    @Override
    public void remove(int index) {
        int childCount = getRawChildPageCount();
        super.remove(index);
        if (isPersistent()) {
            if (map.isMemoryEstimationAllowed()) {
                addMemory(-getMemory() / childCount);
            } else {
                addMemory(-MEMORY_POINTER - PAGE_MEMORY_CHILD);
            }
        }
        totalCount -= children[index].count;
        PageReference<K, V>[] newChildren = createRefStorage(childCount - 1);
        DataUtils.copyExcept(children, newChildren, childCount, index);
        children = newChildren;
    }

    @Override
    public int removeAllRecursive(long version) {
        int unsavedMemory = removePage(version);
        if (isPersistent()) {
            for (int i = 0, size = map.getChildPageCount(this); i < size; i++) {
                PageReference<K, V> ref = children[i];
                Page<K, V> page = ref.getPage();
                if (page != null) {
                    unsavedMemory += page.removeAllRecursive(version);
                } else {
                    long pagePos = ref.getPos();
                    assert DataUtils.isPageSaved(pagePos);
                    if (DataUtils.isLeafPosition(pagePos)) {
                        map.store.accountForRemovedPage(pagePos, version, map.isSingleWriter(), -1);
                    } else {
                        unsavedMemory += map.readPage(pagePos).removeAllRecursive(version);
                    }
                }
            }
        }
        return unsavedMemory;
    }

    @Override
    public CursorPos<K, V> getPrependCursorPos(CursorPos<K, V> cursorPos) {
        Page<K, V> childPage = getChildPage(0);
        return childPage.getPrependCursorPos(new CursorPos<>(this, 0, cursorPos));
    }

    @Override
    public CursorPos<K, V> getAppendCursorPos(CursorPos<K, V> cursorPos) {
        int keyCount = getKeyCount();
        Page<K, V> childPage = getChildPage(keyCount);
        return childPage.getAppendCursorPos(new CursorPos<>(this, keyCount, cursorPos));
    }

    @Override
    protected void readPayLoad(ByteBuffer buff) {
        int keyCount = getKeyCount();
        children = createRefStorage(keyCount + 1);
        long[] p = new long[keyCount + 1];
        for (int i = 0; i <= keyCount; i++) {
            p[i] = buff.getLong();
        }
        long total = 0;
        for (int i = 0; i <= keyCount; i++) {
            long s = DataUtils.readVarLong(buff);
            long position = p[i];
            assert position == 0 ? s == 0 : s >= 0;
            total += s;
            children[i] = position == 0 ?
                    PageReference.empty() :
                    new PageReference<>(position, s);
        }
        totalCount = total;
    }

    @Override
    protected void writeValues(WriteBuffer buff) {
    }

    @Override
    protected void writeChildren(WriteBuffer buff, boolean withCounts) {
        int keyCount = getKeyCount();
        for (int i = 0; i <= keyCount; i++) {
            buff.putLong(children[i].getPos());
        }
        if (withCounts) {
            for (int i = 0; i <= keyCount; i++) {
                buff.putVarLong(children[i].count);
            }
        }
    }

    @Override
    public void writeUnsavedRecursive(FileStore.PageSerializationManager pageSerializationManager) {
        if (!isSaved()) {
            int patch = write(pageSerializationManager);
            writeChildrenRecursive(pageSerializationManager);
            WriteBuffer buff = pageSerializationManager.getBuffer();
            int old = buff.position();
            buff.position(patch);
            writeChildren(buff, false);
            buff.position(old);
        }
    }

    public void writeChildrenRecursive(FileStore.PageSerializationManager pageSerializationManager) {
        int len = getRawChildPageCount();
        for (int i = 0; i < len; i++) {
            PageReference<K, V> ref = children[i];
            Page<K, V> p = ref.getPage();
            if (p != null) {
                p.writeUnsavedRecursive(pageSerializationManager);
                ref.resetPos();
            }
        }
    }

    @Override
    public void releaseSavedPages() {
        int len = getRawChildPageCount();
        for (int i = 0; i < len; i++) {
            children[i].clearPageReference();
        }
    }

    @Override
    public int getRawChildPageCount() {
        return getKeyCount() + 1;
    }

    @Override
    protected int calculateMemory() {
        return super.calculateMemory() + PAGE_NODE_MEMORY +
                getRawChildPageCount() * (MEMORY_POINTER + PAGE_MEMORY_CHILD);
    }

    @Override
    public void dump(StringBuilder buff) {
        super.dump(buff);
        int keyCount = getKeyCount();
        for (int i = 0; i <= keyCount; i++) {
            if (i > 0) {
                buff.append(" ");
            }
            buff.append("[").append(Long.toHexString(children[i].getPos())).append("]");
            if (i < keyCount) {
                buff.append(" ").append(getKey(i));
            }
        }
    }
}
