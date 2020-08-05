package com.adel.batch.processordemo.cassandraTest.cassandra;

import com.adel.batch.processordemo.cassandraTest.cassandra.document.BookDocument;
import com.adel.batch.processordemo.cassandraTest.cassandra.repository.BookRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class AppRunner implements ApplicationRunner {

    @Autowired
    private BookRepository bookRepository;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        bookRepository.deleteAll();
        List<BookDocument> books = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            books.add(new BookDocument(i, RandomStringUtils.randomAlphanumeric(5, 20), RandomStringUtils.randomAlphabetic(7, 15), RandomStringUtils.randomAlphabetic(0, 12)));
        }
        bookRepository.saveAll(books);
        log.info("Finished saving");
    }
}
