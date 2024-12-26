package org.h2.learn;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MVMapTest {
    private final static Logger log = LoggerFactory.getLogger(MVMapTest.class);

    public static void main(String[] args) {
        MVStore s = MVStore.open(null);

        MVMap<Integer, String> map = s.openMap("data");

        for (int i = 0; i < 24; i++) {
            map.put(i, "Hello " + i);
        }

        for (int i = 100; i < 125; i++) {
            map.put(i, "Hello " + i);
        }

        log.info("page: \n{}", map.getRootPage());

        for (int i = 25; i < 50; i++) {
            map.put(i, "Hello " + i);
        }

        log.info("page: \n{}", map.getRootPage());

        long oldVersion = s.getCurrentVersion();
        s.commit(); // from now on, the old version is read-only

        log.info("Map: {}", map);

        map.put(1, "Hi");
        map.remove(2);

        MVMap<Integer, String> oldMap = map.openVersion(oldVersion);

        log.info("Value[1] [{}] on version {}", oldMap.get(1), oldVersion);
        log.info("Value[2] [{}] on version {}", oldMap.get(2), oldVersion);

        log.info("Value[1] [{}] on current version {}", map.get(1), s.getCurrentVersion());
        s.commit();

        log.info("Map: {}", map);

        s.rollbackTo(oldVersion);
        log.info("Value[1] [{}] on current version {}", map.get(1), s.getCurrentVersion());

        log.info("Map: {}", map);

        s.close();
    }
}
