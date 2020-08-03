package com.adel.batch.processordemo.batch.repository.mongo;

import com.adel.batch.processordemo.batch.document.BookDocument;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class BookRepositoryBulkImpl implements BookRepositoryBulk {

    private final MongoTemplate mongoTemplate;

    public BookRepositoryBulkImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }


    @Override
    public List<BookDocument> saveAllBulk(List<BookDocument> books) {
        MongoCollection<Document> collection = mongoTemplate.getCollection("book");
        List<WriteModel<BookDocument>> writes = new ArrayList<>();
        books.forEach(book -> writes.add(new InsertOneModel<>(book)));
        BulkWriteResult bulkWriteResult = collection.bulkWrite(writes);

        return books;
    }

    @Override
    public void deleteAllBulk() {
        MongoCollection<Document> collection = mongoTemplate.getCollection("book");
        collection.drop();
    }


}
