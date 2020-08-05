package com.adel.batch.processordemo.cassandraTest.cassandra.repository;

import com.adel.batch.processordemo.cassandraTest.cassandra.document.BookDocument;

import java.util.List;

public interface BookRepositoryBulk {

    List<BookDocument> saveAllBulk(List<BookDocument> books);
    void deleteAllBulk();
}
