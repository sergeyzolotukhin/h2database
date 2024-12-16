package org.h2.mvstore.page;

import org.h2.mvstore.CursorPos;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.FileStore;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.Page;
import org.h2.mvstore.WriteBuffer;

import java.nio.ByteBuffer;

import static org.h2.engine.Constants.MEMORY_POINTER;
import static org.h2.mvstore.DataUtils.PAGE_TYPE_LEAF;

public class Leaf<K, V> extends Page<K, V> {
    /**
     * The storage for values.
     */
    private V[] values;

    public Leaf(MVMap<K, V> map) {
        super(map);
//            log.info("create page");
    }

    private Leaf(MVMap<K, V> map, Leaf<K, V> source) {
        super(map, source);
        this.values = source.values;
//            log.info("create page");
    }

    public Leaf(MVMap<K, V> map, K[] keys, V[] values) {
        super(map, keys);
        this.values = values;
//            log.info("create page");
    }

    @Override
    public int getNodeType() {
        return PAGE_TYPE_LEAF;
    }

    @Override
    public Page<K, V> copy(MVMap<K, V> map, boolean eraseChildrenRefs) {
        return new Leaf<>(map, this);
    }

    @Override
    public Page<K, V> getChildPage(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getChildPagePos(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V getValue(int index) {
        return values == null ? null : values[index];
    }

    @Override
    public Page<K, V> split(int at) {
        assert !isSaved();
        int b = getKeyCount() - at;
        K[] bKeys = splitKeys(at, b);
        V[] bValues = createValueStorage(b);
        if (values != null) {
            V[] aValues = createValueStorage(at);
            System.arraycopy(values, 0, aValues, 0, at);
            System.arraycopy(values, at, bValues, 0, b);
            values = aValues;
        }
        Page<K, V> newPage = createLeaf(map, bKeys, bValues, 0);
        if (isPersistent()) {
            recalculateMemory();
        }
        return newPage;
    }

    @Override
    public void expand(int extraKeyCount, K[] extraKeys, V[] extraValues) {
        int keyCount = getKeyCount();
        expandKeys(extraKeyCount, extraKeys);
        if (values != null) {
            V[] newValues = createValueStorage(keyCount + extraKeyCount);
            System.arraycopy(values, 0, newValues, 0, keyCount);
            System.arraycopy(extraValues, 0, newValues, keyCount, extraKeyCount);
            values = newValues;
        }
        if (isPersistent()) {
            recalculateMemory();
        }
    }

    @Override
    public long getTotalCount() {
        return getKeyCount();
    }

    @Override
    public long getCounts(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setChild(int index, Page<K, V> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V setValue(int index, V value) {
        values = values.clone();
        V old = setValueInternal(index, value);
        if (isPersistent()) {
            if (!map.isMemoryEstimationAllowed()) {
                addMemory(map.evaluateMemoryForValue(value) -
                        map.evaluateMemoryForValue(old));
            }
        }
        return old;
    }

    private V setValueInternal(int index, V value) {
        V old = values[index];
        values[index] = value;
        return old;
    }

    @Override
    public void insertLeaf(int index, K key, V value) {
        int keyCount = getKeyCount();
        insertKey(index, key);

        if (values != null) {
            V[] newValues = createValueStorage(keyCount + 1);
            DataUtils.copyWithGap(values, newValues, keyCount, index);
            values = newValues;
            setValueInternal(index, value);
            if (isPersistent()) {
                addMemory(MEMORY_POINTER + map.evaluateMemoryForValue(value));
            }
        }
    }

    @Override
    public void insertNode(int index, K key, Page<K, V> childPage) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove(int index) {
        int keyCount = getKeyCount();
        super.remove(index);
        if (values != null) {
            if (isPersistent()) {
                if (map.isMemoryEstimationAllowed()) {
                    addMemory(-getMemory() / keyCount);
                } else {
                    V old = getValue(index);
                    addMemory(-MEMORY_POINTER - map.evaluateMemoryForValue(old));
                }
            }
            V[] newValues = createValueStorage(keyCount - 1);
            DataUtils.copyExcept(values, newValues, keyCount, index);
            values = newValues;
        }
    }

    @Override
    public int removeAllRecursive(long version) {
        return removePage(version);
    }

    @Override
    public CursorPos<K, V> getPrependCursorPos(CursorPos<K, V> cursorPos) {
        return new CursorPos<>(this, -1, cursorPos);
    }

    @Override
    public CursorPos<K, V> getAppendCursorPos(CursorPos<K, V> cursorPos) {
        int keyCount = getKeyCount();
        return new CursorPos<>(this, ~keyCount, cursorPos);
    }

    @Override
    protected void readPayLoad(ByteBuffer buff) {
        int keyCount = getKeyCount();
        values = createValueStorage(keyCount);
        map.getValueType().read(buff, values, getKeyCount());
    }

    @Override
    protected void writeValues(WriteBuffer buff) {
        map.getValueType().write(buff, values, getKeyCount());
    }

    @Override
    protected void writeChildren(WriteBuffer buff, boolean withCounts) {
    }

    @Override
    public void writeUnsavedRecursive(FileStore.PageSerializationManager pageSerializationManager) {
        if (!isSaved()) {
            write(pageSerializationManager);
        }
    }

    @Override
    public void releaseSavedPages() {
    }

    @Override
    public int getRawChildPageCount() {
        return 0;
    }

    @Override
    protected int calculateMemory() {
//*
        return super.calculateMemory() + PAGE_LEAF_MEMORY +
                (values == null ? 0 : map.evaluateMemoryForValues(values, getKeyCount()));
/*/
        int keyCount = getKeyCount();
        int mem = super.calculateMemory() + PAGE_LEAF_MEMORY + keyCount * MEMORY_POINTER;
        DataType<V> valueType = map.getValueType();
        for (int i = 0; i < keyCount; i++) {
            mem += getMemory(valueType, values[i]);
        }
        return mem;
//*/
    }

    @Override
    public void dump(StringBuilder buff) {
        super.dump(buff);
        int keyCount = getKeyCount();
        for (int i = 0; i < keyCount; i++) {
            if (i > 0) {
                buff.append(" ");
            }
            buff.append(getKey(i));
            if (values != null) {
                buff.append(':');
                buff.append(getValue(i));
            }
        }
    }
}
