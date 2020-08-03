package com.adel.batch.processordemo.batch.repository.cassandra;

import com.adel.batch.processordemo.batch.document.BookDocumentCassandra;
import com.datastax.oss.driver.api.core.CqlSession;
import groovy.util.logging.Slf4j;
import org.springframework.data.cassandra.core.CassandraBatchOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
public class BookRepositoryCassandraBulk {

    private final CqlSession cqlSession;

    public BookRepositoryCassandraBulk(CqlSession cqlSession) {
        this.cqlSession = cqlSession;
    }

    public List<BookDocumentCassandra> bulkInsert(List<BookDocumentCassandra> books) {
        CassandraTemplate template = new CassandraTemplate(cqlSession);
        CassandraBatchOperations batchOps = template.batchOps();
        books.forEach(batchOps::insert);
        batchOps.execute();

        return books;
    }

}
