package com.adel.batch.processordemo.batch.repository.mongo;

import com.adel.batch.processordemo.batch.document.mongo.BookDocument;

import java.util.List;

public interface BookRepositoryBulk {
    List<BookDocument> saveAllBulk(List<BookDocument> books);
    void deleteAllBulk();
}
