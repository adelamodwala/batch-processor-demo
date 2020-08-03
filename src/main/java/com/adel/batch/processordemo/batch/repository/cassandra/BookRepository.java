package com.adel.batch.processordemo.batch.repository.cassandra;

import com.adel.batch.processordemo.batch.document.BookDocumentCassandra;
import org.springframework.data.cassandra.repository.CassandraRepository;


public interface BookRepository extends CassandraRepository<BookDocumentCassandra, Long> {
}
