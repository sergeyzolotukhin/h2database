package org.h2.learn;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MVStoreTest {
    private final static Logger log = LoggerFactory.getLogger(MVStoreTest.class);

    public static void main(String[] args) {
        MVStore s = MVStore.open(null);

        MVMap<Integer, String> map = s.openMap("data");
        map.put(1, "Hello World");
        s.commit();

        log.info("{}", map.get(1));


        s.close();
    }
}
