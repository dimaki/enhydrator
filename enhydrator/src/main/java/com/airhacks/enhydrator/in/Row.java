package com.airhacks.enhydrator.in;

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
import com.airhacks.enhydrator.transform.Memory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 *
 * @author airhacks.com
 */
public class Row {

    private final Map<String, Column> columnByName;
    private final Map<Integer, Column> columnByIndex;

    private List<Row> children;
    private Memory memory;

    public Row() {
        this.columnByName = new ConcurrentHashMap<>();
        this.columnByIndex = new ConcurrentHashMap<>();
        this.children = new CopyOnWriteArrayList<>();
    }

    public void useMemory(Memory globalMemory) {
        this.memory = globalMemory;
    }

    /**
     * Get the value of the column with the given name.
     * @param columnName The column name (or the column index, e.g. "15")
     * @return The value of the column
     */
    public Object getColumnValue(String columnName) {
        final Column column = this.columnByName.get(columnName);
        if (column == null || column.isNullValue()) {
            return null;
        }
        return column.getValue();
    }

    /**
     * Get the column with the given name.
     * @param columnName The column name (or the column index, e.g. "15")
     * @return The column
     */
    public Column getColumnByName(String columnName) {
        return this.columnByName.get(columnName);
    }

    public void findColumnsAndApply(Predicate<Column> predicate, Consumer<Column> modifier) {
        this.columnByName.values().stream().filter(predicate).forEach(modifier);
    }

    public void findColumnsAndChangeName(Predicate<Column> predicate, Function<Column, String> renamingFunction) {
        List<Column> collect = this.columnByName.values().stream().
                filter(predicate).collect(Collectors.toList());
        collect.stream().forEach(c -> this.changeColumnName(c.getName(), renamingFunction.apply(c)));

    }

    /**
     * Get the column with the given column index (position)
     * @param index The column index (position). The first column has index 0.
     * @return The column
     */
    public Column getColumnByIndex(int index) {
        return this.columnByIndex.get(index);
    }

    public Collection<Column> getColumns() {
        return this.columnByName.values();
    }

    /**
     * Get all columns sorted ascending by their column index (position)
     * @return a list of all columns
     */
    public List<Column> getColumnsSortedByColumnIndex() {
        return this.columnByName.values().stream().
                sorted((col1, col2) -> Integer.compare(col1.getIndex(), col2.getIndex())).
                collect(Collectors.toList());
    }

    /**
     * Change the name of the column with the given name to the specified new name
     * @param oldName The name of the column to be changed
     * @param newName The new name of the column
     */
    public void changeColumnName(String oldName, String newName) {
        Column column = this.columnByName.remove(oldName);
        if (column == null) {
            return;
        }
        column.setName(newName);
        this.columnByName.put(newName, column);
    }

    /**
     * Add or override a column
     * @param index The index (position) of the column to be added (can be -1 to append to end)
     * @param name A unique name of the column.
     * @param value The value of the column
     * @return the row (containing all columns)
     */
    public Row addColumn(int index, String name, Object value) {
        Objects.requireNonNull(name, "Name of the column cannot be null");
        Objects.requireNonNull(value, "Value of " + name + " cannot be null");
        final Column column = new Column(index, name, value);
        return this.addColumn(column);
    }

    /**
     * Add or override a column
     * @param column The column to be added
     * @return the row (containing all columns)
     */
    public Row addColumn(Column column) {
        Objects.requireNonNull(column, "Column cannot be null");
        this.columnByName.put(column.getName(), column);
        this.columnByIndex.put(column.getIndex(), column);
        return this;
    }

    /**
     * Add or override a column with a NULL value
     * @param index The index (position) of the column to be added (can be -1 to append to end)
     * @param name The name of the column to be added
     * @return the row (containing all columns)
     */
    public Row addNullColumn(int index, String name) {
        final Column column = new Column(index, name);
        this.columnByName.put(name, column);
        this.columnByIndex.put(index, column);
        return this;
    }

    /**
     * Transform a column by applying the given function to it
     * @param columnName The name of the column to be transformed
     * @param transformer The function to apply to the column
     */
    public void transformColumn(String columnName, Function<Object, Object> transformer) {
        Column input = getColumnByName(columnName);
        if (input == null || input.isNullValue()) {
            return;
        }
        Object output = transformer.apply(input.getValue());
        input.setValue(output);
    }

    /**
     * Get number of columns contained in this row
     * @return The number of columns of this row
     */
    public int getNumberOfColumns() {
        return this.columnByName.size();
    }

    /**
     * Get a name-value map of all column names and their column values contained in this row
     * @return A name-value map of all column names and their column values of this row
     */
    public Map<String, Optional<Object>> getColumnValues() {
        return this.columnByName.entrySet().stream().
                collect(Collectors.toMap(k -> k.getKey(), v -> value(v)));
    }

    Optional<Object> value(Entry<String, Column> entry) {
        Objects.requireNonNull(entry, "Entry cannot be null");
        String columnName = entry.getKey();
        Column column = entry.getValue();
        Objects.requireNonNull(columnName, "Column name cannot be null");
        Objects.requireNonNull(column, "Column with name " + columnName + " is null");
        Optional<Object> valueAsOptional = column.getValueAsOptional();
        return valueAsOptional;
    }

    /**
     * Get all column names contained in this row
     * @return All column names of this row
     */
    public Set<String> getColumnNames() {
        return this.columnByName.keySet();
    }

    /** Get a name-value map of all column names and their column values contained in this row. All values are converted to strings.
     * @return A name-value map of all column names and their column values of this row
     */
    public Map<String, String> getColumnsAsString() {
        Map<String, String> retVal = new HashMap<>();
        this.columnByName.keySet().forEach(e -> retVal.put(e, String.valueOf(this.columnByName.get(e))));
        return retVal;
    }

    /**
     * Remove the column with the given name form this row
     * @param name The name of the column to be removed
     * @return the row (containing all columns)
     */
    public Row removeColumn(String name) {
        this.columnByName.remove(name);
        return this;
    }

    public String getDestination(String columnName) {
        return this.columnByName.get(columnName).getTargetSink();
    }

    public boolean isNumber(String column) {
        return (this.columnByName.get(column).isNumber());
    }

    public boolean isString(String column) {
        return (this.columnByName.get(column).isString());
    }

    public Row changeDestination(String column, String newDestination) {
        getColumnByName(column).setTargetSink(newDestination);
        return this;
    }

    /**
     * Check if row is empty (contains no columns)
     * @return true if row is empty, false otherwise
     */
    public boolean isEmpty() {
        return this.columnByName.isEmpty();
    }

    public List<String> getSortedColumnNames() {
        List<String> sortedColumnNames = new ArrayList<>();
        for (int i = 0; i < this.columnByIndex.size(); i++) {
            Column column = this.columnByIndex.get(i);
            if (column == null) {
                sortedColumnNames.add("-");
            } else {
                sortedColumnNames.add(column.getName());
            }
        }
        return sortedColumnNames;
    }

    public Map<String, Row> getColumnsGroupedByDestination() {
        Map<String, List<Map.Entry<String, Column>>> grouped = this.columnByName.entrySet().stream().collect(Collectors.groupingBy(e -> e.getValue().getTargetSink()));
        return grouped.entrySet().stream().
                collect(Collectors.toMap(k -> k.getKey(), v -> convert(v.getValue())));
    }

    public Row convert(List<Map.Entry<String, Column>> content) {
        Row copy = new Row();
        copy.children = this.children;
        content.forEach(c -> copy.addColumn(c.getValue()));
        return copy;
    }

    public boolean isColumnEmpty(String name) {
        return !this.columnByName.containsKey(name);
    }

    /**
     * Add the given row to this row as a child
     * @param child The row to be added as child
     * @return the row (containing the added child)
     */
    public Row add(Row child) {
        this.children.add(child);
        return this;
    }

    /**
     * Get all children (rows) of this row
     * @return a list of all child rows
     */
    public List<Row> getChildren() {
        return this.children;
    }

    /**
     * Check if this row has children
     * @return true if there are child rows, false otherwise
     */
    public boolean hasChildren() {
        return !this.children.isEmpty();
    }

    /**
     * Get the reference to the memory (containing helper functions and processing information)
     * @return The memory
     */
    public Memory getMemory() {
        return memory;
    }

    /**
     * Flag this row to be successfully processed
     */
    public void successfullyProcessed() {
        this.memory.processed();
    }

    /**
     * Flag this row as erronious
     */
    public void errorOccured() {
        this.memory.errorOccured();
    }

    public void errorOccured(Throwable ex) {
        this.memory.addProcessingError(this, ex);
    }

    public void reindexColumns() {
        this.columnByIndex.clear();
        this.columnByName.values().forEach(col -> this.columnByIndex.put(col.getIndex(), col));
    }

    @Override
    public String toString() {
        return "Row{" + "columnByName=" + columnByName + ", columnByIndex=" + columnByIndex + ", children=" + children + '}';
    }
}
