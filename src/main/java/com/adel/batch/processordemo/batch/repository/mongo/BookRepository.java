package com.adel.batch.processordemo.batch.repository.mongo;

import com.adel.batch.processordemo.batch.document.mongo.BookDocument;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

//@Repository
public interface BookRepository extends MongoRepository<BookDocument, String>, BookRepositoryBulk {
}
