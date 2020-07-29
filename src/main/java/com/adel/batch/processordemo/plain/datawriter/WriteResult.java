package com.adel.batch.processordemo.plain.datawriter;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Date;

@Data
@AllArgsConstructor
public class WriteResult {
    private int fileCounter;
    private Date time;
}
