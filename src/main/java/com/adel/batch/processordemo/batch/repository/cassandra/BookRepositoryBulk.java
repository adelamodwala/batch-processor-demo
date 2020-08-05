package com.adel.batch.processordemo.batch.repository.cassandra;

import com.adel.batch.processordemo.batch.document.cassandra.BookDocument;

import java.util.List;

public interface BookRepositoryBulk {

    List<BookDocument> saveAllBulk(List<BookDocument> books);

    void deleteAllBulk();
}
