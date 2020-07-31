package com.adel.batch.processordemo.batch.document;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.mongodb.core.mapping.Document;

import javax.persistence.Id;

@Document(collection = "book")
@Data
@NoArgsConstructor
public class BookDocument extends org.bson.Document {
    @Id
    private String id;
    private long seqId;
    private String name;
    private String author;
    private String category;

    public BookDocument(long seqId, String name, String author, String category) {
        this.seqId = seqId;
        this.name = name;
        this.author = author;
        this.category = category;
    }
}
