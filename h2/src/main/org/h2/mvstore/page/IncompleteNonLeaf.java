package org.h2.mvstore.page;

import org.h2.mvstore.FileStore;
import org.h2.mvstore.MVMap;

import java.util.Arrays;

public class IncompleteNonLeaf<K, V> extends NonLeaf<K, V> {

    private boolean complete;

    public IncompleteNonLeaf(MVMap<K, V> map, NonLeaf<K, V> source) {
        super(map, source, constructEmptyPageRefs(source.getRawChildPageCount()), source.getTotalCount());
//            log.info("create page");
    }

    private static <K, V> PageReference<K, V>[] constructEmptyPageRefs(int size) {
        // replace child pages with empty pages
        PageReference<K, V>[] children = createRefStorage(size);
        Arrays.fill(children, PageReference.empty());
        return children;
    }

    @Override
    public void writeUnsavedRecursive(FileStore.PageSerializationManager pageSerializationManager) {
        if (complete) {
            super.writeUnsavedRecursive(pageSerializationManager);
        } else if (!isSaved()) {
            writeChildrenRecursive(pageSerializationManager);
        }
    }

    @Override
    public boolean isComplete() {
        return complete;
    }

    @Override
    public void setComplete() {
        recalculateTotalCount();
        complete = true;
    }

    @Override
    public void dump(StringBuilder buff) {
        super.dump(buff);
        buff.append(", complete:").append(complete);
    }

}
