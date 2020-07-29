package com.adel.batch.processordemo.plain.datawriter;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.util.StopWatch;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

@Component
@Slf4j
@Profile("writer")
public class DataWriterApplicationRunner implements ApplicationRunner {

    @Override
    public void run(ApplicationArguments args) throws Exception {

        int cores = Runtime.getRuntime().availableProcessors();
        log.info("# Cores: {}", cores);

        int recordsPerFile = 5_000_000;
        int recordsToWrite = 100_000_000;

        int filesToWrite = recordsToWrite / recordsPerFile;

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        // build manifest
        File manifestFile = new File("out/manifest");
        FileOutputStream manifestOs = new FileOutputStream(manifestFile);
        for (int fileCounter = 0; fileCounter < filesToWrite; fileCounter++) {
            manifestOs.write((getOutputFileName(fileCounter) + "\n").getBytes());
        }
        manifestOs.close();

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(cores);
        List<WriteTask> taskList = new ArrayList<>();

        for (int fileCounter = 0; fileCounter < filesToWrite; fileCounter++) {
            taskList.add(new WriteTask(fileCounter, recordsPerFile, fileCounter * recordsPerFile));
        }

        List<Future<WriteResult>> taskResults = null;

        try {
            taskResults = executor.invokeAll(taskList);
        } catch (InterruptedException ex) {
            log.error(String.valueOf(ex));
        }

        executor.shutdown();

        stopWatch.stop();
        log.info("Completed file generation in {}s", stopWatch.getTotalTimeSeconds());
    }

    public static String getOutputFileName(int counter) {
        return "book-records-" + counter + ".avro";
    }
}
