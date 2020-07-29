package com.adel.batch.processordemo.plain.datareader;

import com.adel.batch.processordemo.model.avro.Book;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.util.StopWatch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

@Component
@Slf4j
@Profile("reader")
public class DataReaderApplicationRunner implements ApplicationRunner {

    @Override
    public void run(ApplicationArguments args) throws Exception {
        List<String> manifest = extractManifest();
        manifest.forEach(System.out::println);

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        manifest.stream().forEach(payloadFile -> {
            try {
                InputStream payloadIs = new URL("http://freya.local:3000/" + payloadFile).openStream();
                DatumReader<Book> datumReader = new SpecificDatumReader<>();
                DataFileStream<Book> dataFileStream = new DataFileStream<>(payloadIs, datumReader);

                while(dataFileStream.hasNext()) {
                    Book book = dataFileStream.next();
                    if(book.getSeqId() % 1_000_000 == 0) {
                        System.out.format("%,8d%n", book.getSeqId());
                    }
                }

                dataFileStream.close();
            } catch (IOException e) {
                log.error(String.valueOf(e));
            }
        });

        stopWatch.stop();
        log.info("Completed reading all files in {}s", stopWatch.getTotalTimeSeconds());
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
