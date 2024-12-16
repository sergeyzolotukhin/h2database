package org.h2.mvstore.page;

import org.h2.mvstore.DataUtils;

/**
 * A pointer to a page, either in-memory or using a page position.
 */
public final class PageReference<K, V> {

    /**
     * Singleton object used when arrays of PageReference have not yet been filled.
     */
    @SuppressWarnings("rawtypes")
    public static final PageReference EMPTY = new PageReference<>(null, 0, 0);

    /**
     * The position, if known, or 0.
     */
    private long pos;

    /**
     * The page, if in memory, or null.
     */
    private Page<K, V> page;

    /**
     * The descendant count for this child page.
     */
    public final long count;

    /**
     * Get an empty page reference.
     *
     * @param <X> the key class
     * @param <Y> the value class
     * @return the page reference
     */
    @SuppressWarnings("unchecked")
    public static <X, Y> PageReference<X, Y> empty() {
        return EMPTY;
    }

    public PageReference(Page<K, V> page) {
        this(page, page.getPos(), page.getTotalCount());
    }

    public PageReference(long pos, long count) {
        this(null, pos, count);
        assert DataUtils.isPageSaved(pos);
    }

    private PageReference(Page<K, V> page, long pos, long count) {
        this.page = page;
        this.pos = pos;
        this.count = count;
    }

    public Page<K, V> getPage() {
        return page;
    }

    /**
     * Clear if necessary, reference to the actual child Page object,
     * so it can be garbage collected if not actively used elsewhere.
     * Reference is cleared only if corresponding page was already saved on a disk.
     */
    public void clearPageReference() {
        if (page != null) {
            page.releaseSavedPages();
            assert page.isSaved() || !page.isComplete();
            if (page.isSaved()) {
                assert pos == page.getPos();
                assert count == page.getTotalCount() : count + " != " + page.getTotalCount();
                page = null;
            }
        }
    }

    public long getPos() {
        return pos;
    }

    /**
     * Re-acquire position from in-memory page.
     */
    public void resetPos() {
        Page<K, V> p = page;
        if (p != null && p.isSaved()) {
            pos = p.getPos();
            assert count == p.getTotalCount();
        }
    }

    @Override
    public String toString() {
        return "Cnt:" + count + ", pos:" + (pos == 0 ? "0" : DataUtils.getPageChunkId(pos) +
                (page == null ? "" : "/" + page.pageNo) +
                "-" + DataUtils.getPageOffset(pos) + ":" + DataUtils.getPageMaxLength(pos)) +
                ((page == null ? DataUtils.getPageType(pos) == 0 : page.isLeaf()) ? " leaf" : " node") +
                ", page:{" + page + "}";
    }
}
