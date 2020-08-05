package com.adel.batch.processordemo.cassandraTest.cassandra.repository;

import com.adel.batch.processordemo.cassandraTest.cassandra.document.BookDocument;
import com.datastax.oss.driver.api.core.CqlSession;
import groovy.util.logging.Slf4j;
import org.springframework.data.cassandra.core.CassandraBatchOperations;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
public class BookRepositoryBulkImpl implements BookRepositoryBulk {

    private final CqlSession cqlSession;

    public BookRepositoryBulkImpl(CqlSession cqlSession) {
        this.cqlSession = cqlSession;
    }

    public List<BookDocument> saveAllBulk(List<BookDocument> books) {
        CassandraTemplate template = new CassandraTemplate(cqlSession);
        CassandraBatchOperations batchOps = template.batchOps();
        books.forEach(batchOps::insert);
        batchOps.execute();

        return books;
    }

    public void deleteAllBulk() {
        CassandraOperations cassandraOperations = new CassandraTemplate(cqlSession);
        cassandraOperations.truncate(BookDocument.class);
    }

}
