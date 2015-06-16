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
import com.airhacks.enhydrator.in.Column;
import com.airhacks.enhydrator.in.Row;
import javax.script.Bindings;
import javax.script.ScriptEngineManager;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class ScriptingEnvironmentProviderTest {

    @Test
    public void emptyRowHasMemory() {
        Row row = new Row();
        Memory expected = new Memory();
        row.useMemory(expected);
        Bindings bindings = ScriptingEnvironmentProvider.create(new ScriptEngineManager(), null, row);
        Object actual = bindings.get("$MEMORY");
        assertNotNull(actual);
        assertThat(actual, is(expected));
    }

    /**
     * In case of colums that have no name there should not be a binding in the
     * map of bindings. This test ensures that empty columns do not lead to a
     * "key must not be empty" exception in case of a column without name.
     */
    @Test
    public void testEmptyColumn() {
        Row row = new Row();
        Column emptyColumn = new Column(0, "", "something");
        row.addColumn(emptyColumn);
        Column maxColumn = new Column(1, "max", "somethingBigger");
        row.addColumn(maxColumn);

        Memory expected = new Memory();
        row.useMemory(expected);
        Bindings bindings = ScriptingEnvironmentProvider.create(new ScriptEngineManager(), null, row);
        assertEquals(4, bindings.size());
    }

    @Test
    public void testCaseSenitiveColumnNames() {
        Row row = new Row();
        Column emptyColumn = new Column(0, "max", "something");
        row.addColumn(emptyColumn);
        Column maxColumn = new Column(1, "Max", "somethingBigger");
        row.addColumn(maxColumn);

        Memory expected = new Memory();
        row.useMemory(expected);
        Bindings bindings = ScriptingEnvironmentProvider.create(new ScriptEngineManager(), null, row);
        assertEquals(5, bindings.size());
    }

}
