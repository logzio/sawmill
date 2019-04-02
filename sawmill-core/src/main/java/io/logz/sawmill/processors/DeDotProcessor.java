package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ProcessorProvider(type = "deDot", factory = DeDotProcessor.Factory.class)
public class DeDotProcessor implements Processor {
    private String separator;

    public DeDotProcessor(String separator) {
        this.separator = separator;
    }

    @Override
    public ProcessResult process(Doc doc) throws InterruptedException {
        Map<String,Object> docAsMap = doc.getSource();
        doc.replace(deDotMap(docAsMap));
        return ProcessResult.success();
    }


    private Map<String,Object> deDotMap(Map<String, Object> docAsMap) throws InterruptedException {

        Map<String,Object> mapClone = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : docAsMap.entrySet()) {
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
            String dedotedKey = deDotKey(entry.getKey());
            if (entry.getValue() instanceof Map) {
                Map<String, Object> deDotedMap = deDotMap((Map) entry.getValue());
                mapClone.put(dedotedKey, deDotedMap);
            } else if (isListOfMaps(entry)) {
                deDotListOfMaps(mapClone, entry, dedotedKey);
            } else {
                mapClone.put(dedotedKey, entry.getValue());
            }
        }
        return mapClone;
    }

    private boolean isListOfMaps(Map.Entry<String, Object> entry) {
        return entry.getValue() instanceof List && !((List) entry.getValue()).isEmpty() && ((List) entry.getValue()).get(0) instanceof Map;
    }

    private void deDotListOfMaps(Map<String, Object> mapClone, Map.Entry<String, Object> entry, String dedotedKey) throws InterruptedException {
        List<Map<String,Object>> newInnerMapList = new ArrayList<>();
        mapClone.put(dedotedKey,newInnerMapList);
        for(Object singleMapFromArray : (List)entry.getValue()){
            Map<String,Object> deDotedMap = deDotMap((Map<String, Object>) singleMapFromArray);
            newInnerMapList.add(deDotedMap);
        }
    }

    private String deDotKey(String originalKey) {
        return originalKey.contains(".") ? originalKey.replace(".", separator) : originalKey;
    }

    public static class Factory implements Processor.Factory {

        @Override
        public Processor create(Map<String,Object> config) {
            DeDotProcessor.Configuration dedotConfiguration = JsonUtils.fromJsonMap(DeDotProcessor.Configuration.class, config);
            return new DeDotProcessor(dedotConfiguration.getSeparator());
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String separator ="_";

        public Configuration() { }
        public String getSeparator() {
            return separator;
        }
    }
}