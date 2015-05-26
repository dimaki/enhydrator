package com.airhacks.enhydrator.transform;

/*
 * #%L
 * enhydrator
 * %%
 * Copyright (C) 2014 - 2015 Adam Bien
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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.xml.bind.annotation.adapters.XmlAdapter;

/**
 * @author ZeTo
 */
public class IndexMapperTypeAdapter extends XmlAdapter<String, List<String>>{

    private static final String DELIMITER = ",";

    @Override
    public List<String> unmarshal(String list) throws Exception {
        List<String> arrayList = Arrays.asList(list.split(DELIMITER));
        return arrayList.stream().
                map(s -> s.trim()). // removing trailing and leading blanks
                collect(Collectors.toList());
    }

    @Override
    public String marshal(List<String> list) throws Exception {
        return list.stream().reduce((s1, s2 ) -> s1 + DELIMITER + " " + s2).get();
    }

}
