package com.adel.batch.processordemo.batch.job;

import com.adel.batch.processordemo.batch.repository.BookRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class JobListener extends JobExecutionListenerSupport {

    private final BookRepository bookRepository;

    public JobListener(BookRepository bookRepository) {
        this.bookRepository = bookRepository;
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info("!!! JOB STARTED");
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        if(jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info("!!! JOB FINISHED");
        }
    }
}
