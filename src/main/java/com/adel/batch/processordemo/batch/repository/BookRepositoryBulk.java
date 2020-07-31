package com.adel.batch.processordemo.batch.repository;

import com.adel.batch.processordemo.batch.document.BookDocument;

import java.util.List;

public interface BookRepositoryBulk {
    List<BookDocument> saveAllBulk(List<BookDocument> books);
    void deleteAllBulk();
}
