/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.page;

import static org.h2.engine.Constants.MEMORY_ARRAY;
import static org.h2.engine.Constants.MEMORY_OBJECT;
import static org.h2.engine.Constants.MEMORY_POINTER;
import static org.h2.mvstore.DataUtils.PAGE_TYPE_LEAF;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.h2.compress.Compressor;
import org.h2.mvstore.CursorPos;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.FileStore;
import org.h2.mvstore.FileStore.PageSerializationManager;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.WriteBuffer;
import org.h2.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A page (a node or a leaf).
 * <p>
 * For b-tree nodes, the key at a given index is larger than the largest key of
 * the child at the same index.
 * <p>
 * Serialized format:
 * length of a serialized page in bytes (including this field): int
 * check value: short
 * page number (0-based sequential number within a chunk): varInt
 * map id: varInt
 * number of keys: varInt
 * type: byte (0: leaf, 1: node; +2: compressed)
 * children of the non-leaf node (1 more than keys)
 * compressed: bytes saved (varInt)
 * keys
 * values of the leaf node (one for each key)
 */
public abstract class Page<K,V> implements Cloneable {
    final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * Map this page belongs to
     */
    public final MVMap<K,V> map;

    /**
     * Position of this page's saved image within a Chunk
     * or 0 if this page has not been saved yet
     * or 1 if this page has not been saved yet, but already removed
     * This "removed" flag is to keep track of pages that concurrently
     * changed while they are being stored, in which case the live bookkeeping
     * needs to be aware of this fact.
     * Field needs to be volatile to avoid races between saving thread setting it
     * and other thread reading it to access the page.
     * On top of this update atomicity is required so removal mark and saved position
     * can be set concurrently.
     *
     * @see DataUtils#composePagePos(int, int, int, int) for field format details
     */
    private volatile long pos;

    /**
     * Sequential 0-based number of the page within containing chunk.
     */
    public int pageNo = -1;

    /**
     * The last result of a find operation is cached.
     */
    private int cachedCompare;

    /**
     * The estimated memory used in persistent case, IN_MEMORY marker value otherwise.
     */
    private int memory;

    /**
     * Amount of used disk space by this page only in persistent case.
     */
    private int diskSpaceUsed;

    /**
     * The keys.
     */
    private K[] keys;

    /**
     * Updater for pos field, which can be updated when page is saved,
     * but can be concurrently marked as removed
     */
    @SuppressWarnings("rawtypes")
    private static final AtomicLongFieldUpdater<Page> posUpdater =
                                                AtomicLongFieldUpdater.newUpdater(Page.class, "pos");
    /**
     * The estimated number of bytes used per child entry.
     */
    public static final int PAGE_MEMORY_CHILD = MEMORY_POINTER + 16; //  16 = two longs

    /**
     * The estimated number of bytes used per base page.
     */
    private static final int PAGE_MEMORY =
            MEMORY_OBJECT +           // this
            2 * MEMORY_POINTER +      // map, keys
            MEMORY_ARRAY +            // Object[] keys
            17;                       // pos, cachedCompare, memory, removedInMemory
    /**
     * The estimated number of bytes used per empty internal page object.
     */
    public static final int PAGE_NODE_MEMORY =
            PAGE_MEMORY +             // super
            MEMORY_POINTER +          // children
            MEMORY_ARRAY +            // Object[] children
            8;                        // totalCount

    /**
     * The estimated number of bytes used per empty leaf page.
     */
    public static final int PAGE_LEAF_MEMORY =
            PAGE_MEMORY +             // super
            MEMORY_POINTER +          // values
            MEMORY_ARRAY;             // Object[] values

    /**
     * Marker value for memory field, meaning that memory accounting is replaced by key count.
     */
    private static final int IN_MEMORY = Integer.MIN_VALUE;

    @SuppressWarnings("rawtypes")
    private static final PageReference[] SINGLE_EMPTY = { PageReference.EMPTY };


    public Page(MVMap<K,V> map) {
        this.map = map;
    }

    public Page(MVMap<K,V> map, Page<K,V> source) {
        this(map, source.keys);
        memory = source.memory;
    }

    public Page(MVMap<K,V> map, K[] keys) {
        this.map = map;
        this.keys = keys;
    }

    /**
     * Create a new, empty leaf page.
     *
     * @param <K> key type
     * @param <V> value type
     *
     * @param map the map
     * @return the new page
     */
    public static <K,V> Page<K,V> createEmptyLeaf(MVMap<K,V> map) {
        return createLeaf(map, map.getKeyType().createStorage(0),
                map.getValueType().createStorage(0), PAGE_LEAF_MEMORY);
    }

    /**
     * Create a new, empty internal node page.
     *
     * @param <K> key type
     * @param <V> value type
     *
     * @param map the map
     * @return the new page
     */
    @SuppressWarnings("unchecked")
    public static <K,V> Page<K,V> createEmptyNode(MVMap<K,V> map) {
        return createNode(map, map.getKeyType().createStorage(0), SINGLE_EMPTY, 0,
                            PAGE_NODE_MEMORY + MEMORY_POINTER + PAGE_MEMORY_CHILD); // there is always one child
    }

    /**
     * Create a new non-leaf page. The arrays are not cloned.
     *
     * @param <K> the key class
     * @param <V> the value class
     * @param map the map
     * @param keys the keys
     * @param children the child page positions
     * @param totalCount the total number of keys
     * @param memory the memory used in bytes
     * @return the page
     */
    public static <K,V> Page<K,V> createNode(MVMap<K,V> map, K[] keys, PageReference<K,V>[] children,
                                    long totalCount, int memory) {
        assert keys != null;
        Page<K,V> page = new NonLeaf<>(map, keys, children, totalCount);
        page.initMemoryAccount(memory);
        return page;
    }

    /**
     * Create a new leaf page. The arrays are not cloned.
     *
     * @param <K> key type
     * @param <V> value type
     *
     * @param map the map
     * @param keys the keys
     * @param values the values
     * @param memory the memory used in bytes
     * @return the page
     */
    public static <K,V> Page<K,V> createLeaf(MVMap<K,V> map, K[] keys, V[] values, int memory) {
        assert keys != null;
        Page<K,V> page = new Leaf<>(map, keys, values);
        page.initMemoryAccount(memory);
        return page;
    }

    private void initMemoryAccount(int memoryCount) {
        if(!map.isPersistent()) {
            memory = IN_MEMORY;
        } else if (memoryCount == 0) {
            recalculateMemory();
        } else {
            addMemory(memoryCount);
            assert memoryCount == getMemory();
        }
    }

    /**
     * Get the value for the given key, or null if not found.
     * Search is done in the tree rooted at given page.
     *
     * @param <K> key type
     * @param <V> value type
     *
     * @param key the key
     * @param p the root page
     * @return the value, or null if not found
     */
    public static <K,V> V get(Page<K,V> p, K key) {
        while (true) {
            int index = p.binarySearch(key);
            if (p.isLeaf()) {
                return index >= 0 ? p.getValue(index) : null;
            } else if (index++ < 0) {
                index = -index;
            }
            p = p.getChildPage(index);
        }
    }

    /**
     * Read a page.
     *
     * @param <K> key type
     * @param <V> value type
     *
     * @param buff ByteBuffer containing serialized page info
     * @param pos the position
     * @param map the map
     * @return the page
     */
    public static <K,V> Page<K,V> read(ByteBuffer buff, long pos, MVMap<K,V> map) {
        boolean leaf = (DataUtils.getPageType(pos) & 1) == PAGE_TYPE_LEAF;
        Page<K,V> p = leaf ? new Leaf<>(map) : new NonLeaf<>(map);
        p.pos = pos;
        p.read(buff);
        return p;
    }

    /**
     * Get the id of the page's owner map
     * @return id
     */
    public final int getMapId() {
        return map.getId();
    }

    /**
     * Create a copy of this page with potentially different owning map.
     * This is used exclusively during bulk map copying.
     * Child page references for nodes are cleared (re-pointed to an empty page)
     * to be filled-in later to copying procedure. This way it can be saved
     * mid-process without tree integrity violation
     *
     * @param map new map to own resulting page
     * @param eraseChildrenRefs whether cloned Page should have no child references or keep originals
     * @return the page
     */
    public abstract Page<K,V> copy(MVMap<K, V> map, boolean eraseChildrenRefs);

    /**
     * Get the key at the given index.
     *
     * @param index the index
     * @return the key
     */
    public K getKey(int index) {
        return keys[index];
    }

    /**
     * Get the child page at the given index.
     *
     * @param index the index
     * @return the child page
     */
    public abstract Page<K,V> getChildPage(int index);

    /**
     * Get the position of the child.
     *
     * @param index the index
     * @return the position
     */
    public abstract long getChildPagePos(int index);

    /**
     * Get the value at the given index.
     *
     * @param index the index
     * @return the value
     */
    public abstract V getValue(int index);

    /**
     * Get the number of keys in this page.
     *
     * @return the number of keys
     */
    public final int getKeyCount() {
        return keys.length;
    }

    /**
     * Check whether this is a leaf page.
     *
     * @return true if it is a leaf
     */
    public final boolean isLeaf() {
        return getNodeType() == PAGE_TYPE_LEAF;
    }

    public abstract int getNodeType();

    /**
     * Get the position of the page
     *
     * @return the position
     */
    public final long getPos() {
        return pos;
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        dump(buff);
        return buff.toString();
    }

    /**
     * Dump debug data for this page.
     *
     * @param buff append buffer
     */
    protected void dump(StringBuilder buff) {
        buff.append("type: ").append(getClass().getSimpleName()).append("\n");
        buff.append("id: ").append(System.identityHashCode(this)).append('\n');
        buff.append("pos: ").append(Long.toHexString(pos)).append('\n');
        if (isSaved()) {
            int chunkId = DataUtils.getPageChunkId(pos);
            buff.append("chunk:").append(Long.toHexString(chunkId));
            if (pageNo >= 0) {
                buff.append(",no:").append(Long.toHexString(pageNo));
            }
            buff.append('\n');
        }
    }

    /**
     * Create a copy of this page.
     *
     * @return a mutable copy of this page
     */
    public final Page<K,V> copy() {
        Page<K,V> newPage = clone();
        newPage.pos = 0;
        newPage.pageNo = -1;
        return newPage;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected final Page<K,V> clone() {
        Page<K,V> clone;
        try {
            clone = (Page<K,V>) super.clone();
        } catch (CloneNotSupportedException impossible) {
            throw new RuntimeException(impossible);
        }
        return clone;
    }

    /**
     * Search the key in this page using a binary search. Instead of always
     * starting the search in the middle, the last found index is cached.
     * <p>
     * If the key was found, the returned value is the index in the key array.
     * If not found, the returned value is negative, where -1 means the provided
     * key is smaller than any keys in this page. See also Arrays.binarySearch.
     *
     * @param key the key
     * @return the value or null
     */
    public int binarySearch(K key) {
        int res = map.getKeyType().binarySearch(key, keys, getKeyCount(), cachedCompare);
        cachedCompare = res < 0 ? ~res : res + 1;
        return res;
    }

    /**
     * Split the page. This modifies the current page.
     *
     * @param at the split index
     * @return the page with the entries after the split index
     */
    public abstract Page<K,V> split(int at);

    /**
     * Split the current keys array into two arrays.
     *
     * @param aCount size of the first array.
     * @param bCount size of the second array/
     * @return the second array.
     */
    public K[] splitKeys(int aCount, int bCount) {
        assert aCount + bCount <= getKeyCount();

        K[] aKeys = createKeyStorage(aCount);
        K[] bKeys = createKeyStorage(bCount);
        System.arraycopy(keys, 0, aKeys, 0, aCount);
        System.arraycopy(keys, getKeyCount() - bCount, bKeys, 0, bCount);
        keys = aKeys;
        return bKeys;
    }

    /**
     * Append additional key/value mappings to this Page.
     * New mappings suppose to be in correct key order.
     *
     * @param extraKeyCount number of mappings to be added
     * @param extraKeys to be added
     * @param extraValues to be added
     */
    public abstract void expand(int extraKeyCount, K[] extraKeys, V[] extraValues);

    /**
     * Expand the keys array.
     *
     * @param extraKeyCount number of extra key entries to create
     * @param extraKeys extra key values
     */
    public void expandKeys(int extraKeyCount, K[] extraKeys) {
        int keyCount = getKeyCount();
        K[] newKeys = createKeyStorage(keyCount + extraKeyCount);
        System.arraycopy(keys, 0, newKeys, 0, keyCount);
        System.arraycopy(extraKeys, 0, newKeys, keyCount, extraKeyCount);
        keys = newKeys;
    }

    /**
     * Get the total number of key-value pairs, including child pages.
     *
     * @return the number of key-value pairs
     */
    public abstract long getTotalCount();

    /**
     * Get the number of key-value pairs for a given child.
     *
     * @param index the child index
     * @return the descendant count
     */
    public abstract long getCounts(int index);

    /**
     * Replace the child page.
     *
     * @param index the index
     * @param c the new child page
     */
    public abstract void setChild(int index, Page<K,V> c);

    /**
     * Replace the key at an index in this page.
     *
     * @param index the index
     * @param key the new key
     */
    public final void setKey(int index, K key) {
        keys = keys.clone();
        if(isPersistent()) {
            K old = keys[index];
            if (!map.isMemoryEstimationAllowed() || old == null) {
                int mem = map.evaluateMemoryForKey(key);
                if (old != null) {
                    mem -= map.evaluateMemoryForKey(old);
                }
                addMemory(mem);
            }
        }
        keys[index] = key;
    }

    /**
     * Replace the value at an index in this page.
     *
     * @param index the index
     * @param value the new value
     * @return the old value
     */
    public abstract V setValue(int index, V value);

    /**
     * Insert a key-value pair into this leaf.
     *
     * @param index the index
     * @param key the key
     * @param value the value
     */
    public abstract void insertLeaf(int index, K key, V value);

    /**
     * Insert a child page into this node.
     *
     * @param index the index
     * @param key the key
     * @param childPage the child page
     */
    public abstract void insertNode(int index, K key, Page<K,V> childPage);

    /**
     * Insert a key into the key array
     *
     * @param index index to insert at
     * @param key the key value
     */
    public void insertKey(int index, K key) {
        int keyCount = getKeyCount();
        assert index <= keyCount : index + " > " + keyCount;
        K[] newKeys = createKeyStorage(keyCount + 1);
        DataUtils.copyWithGap(keys, newKeys, keyCount, index);
        keys = newKeys;

        keys[index] = key;

        if (isPersistent()) {
            addMemory(MEMORY_POINTER + map.evaluateMemoryForKey(key));
        }
    }

    /**
     * Remove the key and value (or child) at the given index.
     *
     * @param index the index
     */
    public void remove(int index) {
        int keyCount = getKeyCount();
        if (index == keyCount) {
            --index;
        }
        if(isPersistent()) {
            if (!map.isMemoryEstimationAllowed()) {
                K old = getKey(index);
                addMemory(-MEMORY_POINTER - map.evaluateMemoryForKey(old));
            }
        }
        K[] newKeys = createKeyStorage(keyCount - 1);
        DataUtils.copyExcept(keys, newKeys, keyCount, index);
        keys = newKeys;
    }

    /**
     * Read the page from the buffer.
     *
     * @param buff the buffer to read from
     */
    private void read(ByteBuffer buff) {
        int chunkId = DataUtils.getPageChunkId(pos);
        int offset = DataUtils.getPageOffset(pos);

        int start = buff.position();
        int pageLength = buff.getInt(); // does not include optional part (pageNo)
        int remaining = buff.remaining() + 4;
        if (pageLength > remaining || pageLength < 4) {
            throw DataUtils.newMVStoreException(DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected page length 4..{1}, got {2}", chunkId, remaining,
                    pageLength);
        }

        short check = buff.getShort();
        int checkTest = DataUtils.getCheckValue(chunkId)
                ^ DataUtils.getCheckValue(offset)
                ^ DataUtils.getCheckValue(pageLength);
        if (check != (short) checkTest) {
            throw DataUtils.newMVStoreException(DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected check value {1}, got {2}", chunkId, checkTest, check);
        }

        pageNo = DataUtils.readVarInt(buff);
        if (pageNo < 0) {
            throw DataUtils.newMVStoreException(DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, got negative page No {1}", chunkId, pageNo);
        }

        int mapId = DataUtils.readVarInt(buff);
        if (mapId != map.getId()) {
            throw DataUtils.newMVStoreException(DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected map id {1}, got {2}", chunkId, map.getId(), mapId);
        }

        int keyCount = DataUtils.readVarInt(buff);
        keys = createKeyStorage(keyCount);
        int type = buff.get();
        if(isLeaf() != ((type & 1) == PAGE_TYPE_LEAF)) {
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected node type {1}, got {2}",
                    chunkId, isLeaf() ? "0" : "1" , type);
        }

        // to restrain hacky GenericDataType, which grabs the whole remainder of the buffer
        buff.limit(start + pageLength);

        if (!isLeaf()) {
            readPayLoad(buff);
        }
        boolean compressed = (type & DataUtils.PAGE_COMPRESSED) != 0;
        if (compressed) {
            Compressor compressor;
            if ((type & DataUtils.PAGE_COMPRESSED_HIGH) ==
                    DataUtils.PAGE_COMPRESSED_HIGH) {
                compressor = map.getStore().getCompressorHigh();
            } else {
                compressor = map.getStore().getCompressorFast();
            }
            int lenAdd = DataUtils.readVarInt(buff);
            int compLen = buff.remaining();
            byte[] comp;
            int pos = 0;
            if (buff.hasArray()) {
                comp = buff.array();
                pos = buff.arrayOffset() + buff.position();
            } else {
                comp = Utils.newBytes(compLen);
                buff.get(comp);
            }
            int l = compLen + lenAdd;
            buff = ByteBuffer.allocate(l);
            compressor.expand(comp, pos, compLen, buff.array(),
                    buff.arrayOffset(), l);
        }
        map.getKeyType().read(buff, keys, keyCount);
        if (isLeaf()) {
            readPayLoad(buff);
        }
        diskSpaceUsed = pageLength;
        recalculateMemory();
    }

    /**
     * Read the page payload from the buffer.
     *
     * @param buff the buffer
     */
    protected abstract void readPayLoad(ByteBuffer buff);

    public final boolean isSaved() {
        return DataUtils.isPageSaved(pos);
    }

    public final boolean isRemoved() {
        return DataUtils.isPageRemoved(pos);
    }

    /**
     * Mark this page as removed "in memory". That means that only adjustment of
     * "unsaved memory" amount is required. On the other hand, if page was
     * persisted, it's removal should be reflected in occupancy of the
     * containing chunk.
     *
     * @return true if it was marked by this call or has been marked already,
     *         false if page has been saved already.
     */
    private boolean markAsRemoved() {
        assert getTotalCount() > 0 : this;
        long pagePos;
        do {
            pagePos = pos;
            if (DataUtils.isPageSaved(pagePos)) {
                return false;
            }
            assert !DataUtils.isPageRemoved(pagePos);
        } while (!posUpdater.compareAndSet(this, 0L, 1L));
        return true;
    }

    /**
     * Serializes this page into provided buffer, which represents content of the specified
     * chunk to be persisted and updates the "position" of the page.
     *
     * @param pageSerializationManager which provides a target buffer
     *                                and can be queried for various attributes
     *                                related to serialization
     * @return the position of the buffer, where serialized child page references (if any) begin
     */
    protected final int write(FileStore<?>.PageSerializationManager pageSerializationManager) {
        pageNo = pageSerializationManager.getPageNo();
        int keyCount = getKeyCount();
        WriteBuffer buff = pageSerializationManager.getBuffer();
        int start = buff.position();
        buff.putInt(0)          // placeholder for pageLength
            .putShort((byte)0) // placeholder for check
            .putVarInt(pageNo)
            .putVarInt(map.getId())
            .putVarInt(keyCount);
        int typePos = buff.position();
        int type = isLeaf() ? PAGE_TYPE_LEAF : DataUtils.PAGE_TYPE_NODE;
        buff.put((byte)type);
        int childrenPos = buff.position();
        writeChildren(buff, true);
        int compressStart = buff.position();
        map.getKeyType().write(buff, keys, keyCount);
        writeValues(buff);
        MVStore store = map.getStore();
        int expLen = buff.position() - compressStart;
        if (expLen > 16) {
            int compressionLevel = store.getCompressionLevel();
            if (compressionLevel > 0) {
                Compressor compressor;
                int compressType;
                if (compressionLevel == 1) {
                    compressor = store.getCompressorFast();
                    compressType = DataUtils.PAGE_COMPRESSED;
                } else {
                    compressor = store.getCompressorHigh();
                    compressType = DataUtils.PAGE_COMPRESSED_HIGH;
                }
                byte[] comp = new byte[expLen * 2];
                ByteBuffer byteBuffer = buff.getBuffer();
                int pos = 0;
                byte[] exp;
                if (byteBuffer.hasArray()) {
                    exp = byteBuffer.array();
                    pos = byteBuffer.arrayOffset()  + compressStart;
                } else {
                    exp = Utils.newBytes(expLen);
                    buff.position(compressStart).get(exp);
                }
                int compLen = compressor.compress(exp, pos, expLen, comp, 0);
                int plus = DataUtils.getVarIntLen(expLen - compLen);
                if (compLen + plus < expLen) {
                    buff.position(typePos)
                        .put((byte) (type | compressType));
                    buff.position(compressStart)
                        .putVarInt(expLen - compLen)
                        .put(comp, 0, compLen);
                }
            }
        }
        int pageLength = buff.position() - start;
        long pagePos = pageSerializationManager.getPagePosition(getMapId(), start, pageLength, type);
        if (isSaved()) {
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_INTERNAL, "Page already stored");
        }
        boolean isDeleted = isRemoved();
        while (!posUpdater.compareAndSet(this, isDeleted ? 1L : 0L, pagePos)) {
            isDeleted = isRemoved();
        }
        int pageLengthDecoded = DataUtils.getPageMaxLength(pagePos);
        diskSpaceUsed = pageLengthDecoded != DataUtils.PAGE_LARGE ? pageLengthDecoded : pageLength;
        boolean singleWriter = map.isSingleWriter();

        pageSerializationManager.onPageSerialized(this, isDeleted, pageLengthDecoded, singleWriter);
        return childrenPos;
    }

    /**
     * Write values that the buffer contains to the buff.
     *
     * @param buff the target buffer
     */
    protected abstract void writeValues(WriteBuffer buff);

    /**
     * Write page children to the buff.
     *
     * @param buff the target buffer
     * @param withCounts true if the descendant counts should be written
     */
    protected abstract void writeChildren(WriteBuffer buff, boolean withCounts);

    /**
     * Store this page and all children that are changed, in reverse order, and
     * update the position and the children.
     *
     * @param pageSerializationManager which provides a target buffer
     *                                and can be queried for various attributes
     *                                related to serialization
     */
    public abstract void writeUnsavedRecursive(PageSerializationManager pageSerializationManager);

    /**
     * Unlink the children recursively after all data is written.
     */
    public abstract void releaseSavedPages();

    public abstract int getRawChildPageCount();

    protected final boolean isPersistent() {
        return memory != IN_MEMORY;
    }

    public final int getMemory() {
        if (isPersistent()) {
//            assert memory == calculateMemory() :
//                    "Memory calculation error " + memory + " != " + calculateMemory();
            return memory;
        }
        return 0;
    }

    /**
     * Amount of used disk space in persistent case including child pages.
     *
     * @param approximate
     *            {@code true} to return quick approximation
     *
     * @return amount of used disk space in persistent case
     */
    public final long getDiskSpaceUsed(boolean approximate) {
        return isPersistent() //
                ? approximate ? getDiskSpaceUsedApproximation(3, false) : getDiskSpaceUsedAccurate()
                : 0L;
    }

    private long getDiskSpaceUsedAccurate() {
        long r = diskSpaceUsed;
        if (!isLeaf()) {
            for (int i = 0, l = getRawChildPageCount(); i < l; i++) {
                long pos = getChildPagePos(i);
                if (pos != 0) {
                    r += getChildPage(i).getDiskSpaceUsedAccurate();
                }
            }
        }
        return r;
    }

    private long getDiskSpaceUsedApproximation(int maxLevel, boolean f) {
        long r = diskSpaceUsed;
        if (!isLeaf()) {
            int l = getRawChildPageCount();
            if (--maxLevel == 0 && l > 4) {
                if (f) {
                    for (int i = 0; i < l; i++) {
                        long pos = getChildPagePos(i);
                        if (pos != 0) {
                            r += getChildPage(i).getDiskSpaceUsedApproximation(maxLevel, f) * l;
                            break;
                        }
                    }
                } else {
                    for (int i = l; --i >= 0;) {
                        long pos = getChildPagePos(i);
                        if (pos != 0) {
                            r += getChildPage(i).getDiskSpaceUsedApproximation(maxLevel, f) * l;
                            break;
                        }
                    }
                }
            } else {
                for (int i = 0; i < l; i++) {
                    long pos = getChildPagePos(i);
                    if (pos != 0) {
                        r += getChildPage(i).getDiskSpaceUsedApproximation(maxLevel, f);
                        f = !f;
                    }
                }
            }
        }
        return r;
    }

    /**
     * Increase estimated memory used in persistent case.
     *
     * @param mem additional memory size.
     */
    public void addMemory(int mem) {
        memory += mem;
        assert memory >= 0;
    }

    /**
     * Recalculate estimated memory used in persistent case.
     */
    public void recalculateMemory() {
        assert isPersistent();
        memory = calculateMemory();
    }

    /**
     * Calculate estimated memory used in persistent case.
     *
     * @return memory in bytes
     */
    protected int calculateMemory() {
//*
        return map.evaluateMemoryForKeys(keys, getKeyCount());
/*/
        int keyCount = getKeyCount();
        int mem = keyCount * MEMORY_POINTER;
        DataType<K> keyType = map.getKeyType();
        for (int i = 0; i < keyCount; i++) {
            mem += getMemory(keyType, keys[i]);
        }
        return mem;
//*/
    }

    public boolean isComplete() {
        return true;
    }

    /**
     * Called when done with copying page.
     */
    public void setComplete() {}

    /**
     * Make accounting changes (chunk occupancy or "unsaved" RAM), related to
     * this page removal.
     *
     * @param version at which page was removed
     * @return amount (negative), by which "unsaved memory" should be adjusted,
     *         if page is unsaved one, and 0 for page that was already saved, or
     *         in case of non-persistent map
     */
    public final int removePage(long version) {
        if(isPersistent() && getTotalCount() > 0) {
            MVStore store = map.store;
            if (!markAsRemoved()) { // only if it has been saved already
                long pagePos = pos;
                store.accountForRemovedPage(pagePos, version, map.isSingleWriter(), pageNo);
            } else {
                return -memory;
            }
        }
        return 0;
    }

    /**
     * Extend path from a given CursorPos chain to "prepend point" in a B-tree, rooted at this Page.
     *
     * @param cursorPos presumably pointing to this Page (null if real root), to build upon
     * @return new head of the CursorPos chain
     */
    public abstract CursorPos<K,V> getPrependCursorPos(CursorPos<K,V> cursorPos);

    /**
     * Extend path from a given CursorPos chain to "append point" in a B-tree, rooted at this Page.
     *
     * @param cursorPos presumably pointing to this Page (null if real root), to build upon
     * @return new head of the CursorPos chain
     */
    public abstract CursorPos<K,V> getAppendCursorPos(CursorPos<K,V> cursorPos);

    /**
     * Remove all page data recursively.
     * @param version at which page got removed
     * @return adjustment for "unsaved memory" amount
     */
    public abstract int removeAllRecursive(long version);

    /**
     * Create array for keys storage.
     *
     * @param size number of entries
     * @return values array
     */
    public final K[] createKeyStorage(int size) {
        return map.getKeyType().createStorage(size);
    }

    /**
     * Create array for values storage.
     *
     * @param size number of entries
     * @return values array
     */
    public V[] createValueStorage(int size) {
        return map.getValueType().createStorage(size);
    }

    /**
     * Create an array of page references.
     *
     * @param <K> the key class
     * @param <V> the value class
     * @param size the number of entries
     * @return the array
     */
    @SuppressWarnings("unchecked")
    public static <K,V> PageReference<K,V>[] createRefStorage(int size) {
        return new PageReference[size];
    }


}
