package com.adel.batch.processordemo.batch.job;

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
 * A {@link org.springframework.batch.item.ItemWriter} wrapper for a
 * {@link org.springframework.data.repository.CrudRepository} from Spring Data.
 * </p>
 *
 * <p>
 * By default, this writer will use {@link CrudRepository#saveAll(Iterable)}
 * to save items, unless another method is selected with {@link #setMethodName(java.lang.String)}.
 * It depends on {@link org.springframework.data.repository.CrudRepository#saveAll(Iterable)}
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
public class BatchRepositoryItemWriter<T> implements ItemWriter<T>, InitializingBean {

    protected static final Log logger = LogFactory.getLog(RepositoryItemWriter.class);

    private CrudRepository<T, ?> repository;

    private String methodName;

    /**
     * Specifies what method on the repository to call.  This method must have the type of
     * object passed to this writer as the <em>sole</em> argument.
     *
     * @param methodName {@link String} containing the method name.
     */
    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    /**
     * Set the {@link org.springframework.data.repository.CrudRepository} implementation
     * for persistence
     *
     * @param repository the Spring Data repository to be set
     */
    public void setRepository(CrudRepository<T, ?> repository) {
        this.repository = repository;
    }

    /**
     * Write all items to the data store via a Spring Data repository.
     *
     * @see org.springframework.batch.item.ItemWriter#write(java.util.List)
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

        if (this.methodName == null) {
            this.repository.saveAll(items);
            return;
        }

        MethodInvoker invoker = createMethodInvoker(repository, methodName);

        for (T object : items) {
            invoker.setArguments(new Object[]{object});
            doInvoke(invoker);
        }
    }

    /**
     * Check mandatory properties - there must be a repository.
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.state(repository != null, "A CrudRepository implementation is required");
        if (this.methodName != null) {
            Assert.hasText(this.methodName, "methodName must not be empty.");
        } else {
            logger.debug("No method name provided, CrudRepository.saveAll will be used.");
        }
    }


    private Object doInvoke(MethodInvoker invoker) throws Exception {
        try {
            invoker.prepare();
        } catch (ClassNotFoundException e) {
            throw new DynamicMethodInvocationException(e);
        } catch (NoSuchMethodException e) {
            throw new DynamicMethodInvocationException(e);
        }

        try {
            return invoker.invoke();
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            } else {
                throw new InvocationTargetThrowableWrapper(e.getCause());
            }
        } catch (IllegalAccessException e) {
            throw new DynamicMethodInvocationException(e);
        }
    }

    private MethodInvoker createMethodInvoker(Object targetObject, String targetMethod) {
        MethodInvoker invoker = new MethodInvoker();
        invoker.setTargetObject(targetObject);
        invoker.setTargetMethod(targetMethod);
        return invoker;
    }
}
