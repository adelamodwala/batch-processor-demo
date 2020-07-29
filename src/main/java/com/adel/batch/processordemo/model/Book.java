package com.adel.batch.processordemo.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Book {
    private long seqId;
    private String name;
    private String author;
    private String category;
}
