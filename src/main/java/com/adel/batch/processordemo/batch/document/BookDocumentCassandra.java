package com.adel.batch.processordemo.batch.document;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Table(value = "users")
public class BookDocumentCassandra {
    @PrimaryKey("seq_id") private long seqId;
    private String name;
    private String author;
    private String category;
}
