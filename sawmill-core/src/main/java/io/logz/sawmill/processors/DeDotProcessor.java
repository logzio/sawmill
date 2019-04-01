package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ProcessorProvider(type = "de_dot", factory = DeDotProcessor.Factory.class)
public class DeDotProcessor implements Processor {
    private String fieldSplit;

    public DeDotProcessor(String fieldSplit) {
        this.fieldSplit = fieldSplit;
    }

    @Override
    public ProcessResult process(Doc doc) throws InterruptedException {
        Map<String,Object> docAsMap = doc.getSource();
        //Creating a copy of the Map in this level , and also in each level in the recursive level is required
        //in order to avoid ConcurrentModificationException , because in our case we want to replace the key thus actually we remove & add .
        //add is not supported while iterating , and the end result is to create a copy of the map.
        Map<String,Object> dedotedMap = new LinkedHashMap<>();
        deDotMap(docAsMap,dedotedMap);
        doc.replace(dedotedMap);
        return ProcessResult.success();
    }


    private void deDotMap(Map<String, Object> docAsMap, Map<String,Object> mapClone) throws InterruptedException {
        Iterator<Map.Entry<String,Object>> iterator = docAsMap.entrySet().iterator();

        while(iterator.hasNext()){
            if (Thread.interrupted()) throw new InterruptedException();
            Map.Entry<String,Object> entry = iterator.next();
            String dedotedKey = deDotString(entry.getKey());
            if(entry.getValue() instanceof Map){
                Map<String,Object> newInnerMap = new LinkedHashMap<>();
                mapClone.put(dedotedKey,newInnerMap);
                deDotMap((Map)entry.getValue(),newInnerMap);
            }
            else if(entry.getValue() instanceof List && !((List) entry.getValue()).isEmpty() && ((List) entry.getValue()).get(0) instanceof Map){
                List<Map<String,Object>> newInnerMapList = new ArrayList<>();
                mapClone.put(dedotedKey,newInnerMapList);
                for(Object singleMapFromArray : (List)entry.getValue()){
                    Map<String,Object> newInnerMap = new LinkedHashMap<>();
                    newInnerMapList.add(newInnerMap);
                    deDotMap((Map<String, Object>) singleMapFromArray,newInnerMap);
                }
            }else{
                mapClone.put(dedotedKey,entry.getValue());
            }
        }
    }

    private String deDotString(String originalKey) {
        String dedotedKey = originalKey;
        if(dedotedKey.contains(".")){
            dedotedKey = dedotedKey.replace(".",fieldSplit);
        }
        return dedotedKey;
    }

    public static class Factory implements Processor.Factory {

        @Override
        public Processor create(Map<String,Object> config) {
            DeDotProcessor.Configuration dedotConfiguration = JsonUtils.fromJsonMap(DeDotProcessor.Configuration.class, config);
            return new DeDotProcessor(dedotConfiguration.getSeperator());
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String seperator="_";

        public Configuration() { }

        public Configuration(String seperator) {
            this.seperator = seperator;
        }
        public String getSeperator() {
            return seperator;
        }
    }
}