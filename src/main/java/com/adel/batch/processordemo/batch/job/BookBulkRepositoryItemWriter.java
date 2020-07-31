package com.adel.batch.processordemo.batch.job;

import com.adel.batch.processordemo.batch.document.BookDocument;
import com.adel.batch.processordemo.batch.repository.BookRepository;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.adapter.AbstractMethodInvokingDelegator.InvocationTargetThrowableWrapper;
import org.springframework.batch.item.adapter.DynamicMethodInvocationException;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.repository.CrudRepository;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.MethodInvoker;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * <p>
 * A {@link ItemWriter} wrapper for a
 * {@link CrudRepository} from Spring Data.
 * </p>
 *
 * <p>
 * By default, this writer will use {@link CrudRepository#saveAll(Iterable)}
 * to save items, unless another method is selected with {@link #setMethodName(String)}.
 * It depends on {@link CrudRepository#saveAll(Iterable)}
 * method to store the items for the chunk.  Performance will be determined by that
 * implementation more than this writer.
 * </p>
 *
 * <p>
 * As long as the repository provided is thread-safe, this writer is also thread-safe once
 * properties are set (normal singleton behavior), so it can be used in multiple concurrent
 * transactions.
 * </p>
 *
 * <p>
 * NOTE: The {@code RepositoryItemWriter} only stores Java Objects i.e. non primitives.
 * </p>
 *
 * @author Michael Minella
 * @author Mahmoud Ben Hassine
 * @since 2.2
 */
public class BookBulkRepositoryItemWriter<T> implements ItemWriter<T>, InitializingBean {

    protected static final Log logger = LogFactory.getLog(RepositoryItemWriter.class);

    private BookRepository repository;

    /**
     * Set the {@link CrudRepository} implementation
     * for persistence
     *
     * @param repository the Spring Data repository to be set
     */
    public void setRepository(BookRepository repository) {
        this.repository = repository;
    }

    /**
     * Write all items to the data store via a Spring Data repository.
     *
     * @see ItemWriter#write(List)
     */
    @Override
    public void write(List<? extends T> items) throws Exception {
        if (!CollectionUtils.isEmpty(items)) {
            doWrite(items);
        }
    }

    /**
     * Performs the actual write to the repository.  This can be overridden by
     * a subclass if necessary.
     *
     * @param items the list of items to be persisted.
     * @throws Exception thrown if error occurs during writing.
     */
    protected void doWrite(List<? extends T> items) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("Writing to the repository with " + items.size() + " items.");
        }

        repository.saveAllBulk((List<BookDocument>) items);
    }

    /**
     * Check mandatory properties - there must be a repository.
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.state(repository != null, "A CrudRepository implementation is required");
    }
}
