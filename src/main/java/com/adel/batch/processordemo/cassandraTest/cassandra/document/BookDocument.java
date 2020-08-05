package com.adel.batch.processordemo.cassandraTest.cassandra.document;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Table(value = "books")
public class BookDocument {
    @PrimaryKey("seq_id")
    private long seqId;
    private String name;
    private String author;
    private String category;

    public static String tableName() {
        return "books";
    }
}
