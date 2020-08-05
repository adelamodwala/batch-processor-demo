package com.adel.batch.processordemo.batch.repository.cassandra;

import com.adel.batch.processordemo.cassandraTest.cassandra.document.BookDocument;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface BookRepositoryCassandra extends CassandraRepository<BookDocument, Long>, BookRepositoryBulk {
}
