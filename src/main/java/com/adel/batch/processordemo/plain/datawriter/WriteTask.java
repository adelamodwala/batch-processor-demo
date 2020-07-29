package com.adel.batch.processordemo.plain.datawriter;

import com.adel.batch.processordemo.model.avro.Book;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.concurrent.Callable;

@Slf4j
public class WriteTask implements Callable<WriteResult> {

    private final int fileCounter;
    private final int recordsToWrite;
    private long blockSeqId;

    public WriteTask(int fileCounter, int recordsToWrite, long blockSeqId) {
        this.fileCounter = fileCounter;
        this.recordsToWrite = recordsToWrite;
        this.blockSeqId = blockSeqId;
    }

    @Override
    public WriteResult call() throws Exception {
        // File level
        GenericDatumWriter<Book> bookDatumWriter = new GenericDatumWriter<>(Book.SCHEMA$);
        DataFileWriter<Book> dataFileWriter = new DataFileWriter<>(bookDatumWriter);
        dataFileWriter.setCodec(CodecFactory.snappyCodec());

        String fileFolderName = "out";
        String fileName = DataWriterApplicationRunner.getOutputFileName(fileCounter);
        log.info("Generating file {} with {} records", fileName, recordsToWrite);

        File file = new File(fileFolderName + "/" + fileName);
        FileOutputStream fileOutputStream = null;
        try {
            fileOutputStream = new FileOutputStream(file);
        } catch (FileNotFoundException e) {
            log.error(String.valueOf(e));
        }
        try {
            dataFileWriter.create(Book.SCHEMA$, fileOutputStream);

            for (int i = 0; i < recordsToWrite; i++) {
                dataFileWriter.append(generateBook(blockSeqId));
                blockSeqId++;
            }

            dataFileWriter.close();
        } catch (IOException e) {
            log.error(String.valueOf(e));
        }

        log.info("Finished generating file {}", fileName);

        return new WriteResult(fileCounter, new Date());
    }

    private static Book generateBook(long id) {
        return new Book(id, RandomStringUtils.randomAlphanumeric(5, 20), RandomStringUtils.randomAlphabetic(7, 15), RandomStringUtils.randomAlphabetic(0, 12));
    }
}
