/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.h2.tools.DeleteDbFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A very simple class that shows how to load the driver, create a database,
 * create a table, and insert some data.
 */
public class HelloWorld {
    private final static Logger log = LoggerFactory.getLogger(HelloWorld.class);

    /**
     * Called when ran from command line.
     *
     * @param args ignored
     * @throws Exception on failure
     */
    public static void main(String... args) throws Exception {
        // delete the database named 'test' in the user home directory
        DeleteDbFiles.execute("~", "test", true);

        Class.forName("org.h2.Driver");
//        Connection conn = DriverManager.getConnection("jdbc:h2:~/test");
        Connection conn = DriverManager.getConnection("jdbc:h2:mem:;AUTOCOMMIT=OFF");
//        log.info("=== Connected to database ===\n");
        Statement stat = conn.createStatement();

        // this line would initialize the database
        // from the SQL script file 'init.sql'
        // stat.execute("runscript from 'init.sql'");

        log.info("=== create table===\n");

        stat.execute("create table test(id int primary key, name varchar(255))");
//        stat.execute("create table test_2(id int primary key, name varchar(255))");
//        stat.execute("create table test_3(id int primary key, name varchar(255))");

        log.info("=== Insert ===\n");
        stat.execute("insert into test values(1, 'Hello')");
//        stat.execute("insert into test values(2, 'Hello')");

        log.info("=== Commit ===\n");
        conn.commit();
//        stat.execute("create table test_3(id int primary key, name varchar(255))");
//        stat.execute("insert into test values(2, 'Hello')");
//        conn.commit();

        log.info("=== Query ===\n");
        ResultSet rs;
        rs = stat.executeQuery("select * from test");
        while (rs.next()) {
            log.info(rs.getString("name"));
        }
        stat.close();
        conn.close();
    }

}
