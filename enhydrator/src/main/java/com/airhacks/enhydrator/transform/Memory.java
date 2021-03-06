package com.airhacks.enhydrator.transform;

/*
 * #%L
 * enhydrator
 * %%
 * Copyright (C) 2014 Adam Bien
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
import com.airhacks.enhydrator.in.Row;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 *
 * @author airhacks.com
 */
public class Memory {

    private final Map<String, Object> store;
    private final Map<Row, Throwable> processingErrors;

    private final LongAdder counter;
    private final LongAdder processedRowCount;
    private final LongAdder errorCount;

    public Memory() {
        this.store = new ConcurrentHashMap<>();
        this.counter = new LongAdder();
        this.processedRowCount = new LongAdder();
        this.errorCount = new LongAdder();
        this.processingErrors = new ConcurrentHashMap<>();
    }

    /**
     * Store the given value identified by the given key in the memory
     * @param key The key (identifier)
     * @param value The value
     * @return The name-value map of all entires in the memory
     */
    public Map<String, Object> put(String key, Object value) {
        this.store.put(key, value);
        return this.store;
    }

    /**
     * Increment the counter value
     */
    public void increment() {
        counter.increment();
    }

    /**
     * Decrement the counter value
     */
    public void decrement() {
        counter.decrement();
    }

    /**
     * Get the current counter value
     * @return The current counter value
     */
    public long counterValue() {
        return counter.longValue();
    }

    /**
     * Get the value identified by the given key from the memory
     * @param key The key (identifier)
     * @return The value
     */
    public Object get(String key) {
        return this.store.get(key);
    }

    public void processed() {
        this.processedRowCount.increment();
    }

    public void errorOccured() {
        this.errorCount.increment();
    }

    /**
     * Get number of currently processed rows
     * @return Number of processed rows
     */
    public long getProcessedRowCount() {
        return this.processedRowCount.longValue();
    }

    /**
     * Get number of rows that had errors
     * @return Number of rows with errors
     */
    public long getErroneousRowCount() {
        return this.errorCount.longValue();
    }

    public void addProcessingError(Row erroneous, Throwable ex) {
        this.processingErrors.put(erroneous, ex);
        this.errorOccured();
    }

    /**
     * Check if there were any errors
     * @return true if there were processing errors, false otherwise
     */
    public boolean areErrorsOccured() {
        return !this.processingErrors.isEmpty();
    }

    public Collection<Throwable> getProcessingErrors() {
        return this.processingErrors.values();
    }

    /**
     * Get all rows that had processing errors
     * @return All rows with processing errors
     */
    public Set<Row> getErroneousRows() {
        return this.processingErrors.keySet();
    }

    @Override
    public String toString() {
        return "Memory{" + "store=" + store + ", processingErrors=" + processingErrors + ", counter=" + counter + ", processedRowCount=" + processedRowCount + ", errorCount=" + errorCount + '}';
    }
}
