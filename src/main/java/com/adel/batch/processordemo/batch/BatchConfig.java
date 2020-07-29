package com.adel.batch.processordemo.batch;

import com.adel.batch.processordemo.batch.document.BookDocument;
import com.adel.batch.processordemo.batch.job.BatchRepositoryItemWriter;
import com.adel.batch.processordemo.batch.job.JobListener;
import com.adel.batch.processordemo.batch.repository.BookRepository;
import com.adel.batch.processordemo.model.avro.Book;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.MultiResourcePartitioner;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.avro.AvroItemReader;
import org.springframework.batch.item.avro.builder.AvroItemReaderBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

@Configuration
@EnableBatchProcessing
@Slf4j
public class BatchConfig {

    public JobBuilderFactory jobBuilderFactory;
    public StepBuilderFactory stepBuilderFactory;
    public BookRepository bookRepository;

    @Autowired
    private AvroItemReader<Book> avroItemReader;

    public BatchConfig(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, BookRepository bookRepository) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.bookRepository = bookRepository;
    }

    @StepScope
    @Bean
    @Qualifier("avroItemReader")
    public AvroItemReader<Book> itemReader(@Value("#{stepExecutionContext['fileName']}") String filename) throws MalformedURLException {
        return new AvroItemReaderBuilder<Book>()
                .type(Book.class)
                .resource(new UrlResource(filename))
                .build();
    }

    @StepScope
    @Bean
    public ItemProcessor<Book, BookDocument> processor() {
        return item -> new BookDocument(item.getSeqId(), item.getName().toString(), item.getAuthor().toString(), item.getCategory().toString());
    }

    @StepScope
    @Bean
    public ItemWriter<BookDocument> itemWriter() {
        BatchRepositoryItemWriter<BookDocument> batchWriter = new BatchRepositoryItemWriter<>();
        batchWriter.setRepository(bookRepository);
        return batchWriter;
    }

    @Bean
    public Partitioner partitioner() {
        List<String> fileNames = new ArrayList<>();
        try {
            fileNames = extractManifest();
        } catch (IOException e) {
            log.error("Is the file server running?", e);
        }

        MultiResourcePartitioner partitioner = new MultiResourcePartitioner();
        Resource[] resources = new Resource[fileNames.size()];
        for(int i = 0; i < fileNames.size(); i++) {
            try {
                resources[i] = new UrlResource("http://freya.local:3000/" + fileNames.get(i));
            } catch (MalformedURLException e) {
                log.error(String.valueOf(e));
            }
        }

        partitioner.setResources(resources);
        partitioner.partition(16);
        return partitioner;
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .tasklet(((stepContribution, chunkContext) -> {
                    bookRepository.deleteAll();
                    return RepeatStatus.FINISHED;
                }))
                .build();
    }

    @Bean
    public Step step2() {
        return stepBuilderFactory.get("step2")
                .<Book, BookDocument>chunk(100)
                .reader(avroItemReader)
                .processor(processor())
                .writer(itemWriter())
                .build();
    }

    @Bean
    public Step masterStep() {
        return stepBuilderFactory.get("masterStep")
                .partitioner("step2", partitioner())
                .step(step2())
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean
    public ThreadPoolTaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(16);
        taskExecutor.setCorePoolSize(16);
        taskExecutor.afterPropertiesSet();
        return taskExecutor;
    }

    @Bean
    public Job importBooksJob(JobListener listener) throws IOException {
        return jobBuilderFactory.get("importBooksJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step1())
                .next(masterStep())
                .end()
                .build();
    }

    private List<String> extractManifest() throws IOException {
        InputStream manifestIs = new URL("http://freya.local:3000/manifest").openStream();
        BufferedReader manifestReader = new BufferedReader(new InputStreamReader(manifestIs));
        String line;
        List<String> result = new ArrayList<>();
        while ((line = manifestReader.readLine()) != null) {
            result.add(line.trim());
        }
        manifestReader.close();

        return result;
    }
}
