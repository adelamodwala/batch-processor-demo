package com.adel.batch.processordemo.batch.repository;

import java.util.List;

public interface BulkRepository {
    <T> List<T> saveAllBulk(List<T> entities);
}
