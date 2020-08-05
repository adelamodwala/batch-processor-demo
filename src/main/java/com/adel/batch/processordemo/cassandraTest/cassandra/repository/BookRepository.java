package com.adel.batch.processordemo.cassandraTest.cassandra.repository;

import com.adel.batch.processordemo.cassandraTest.cassandra.document.BookDocument;
import org.springframework.data.cassandra.repository.CassandraRepository;


public interface BookRepository extends CassandraRepository<BookDocument, Long>, BookRepositoryBulk {
}
