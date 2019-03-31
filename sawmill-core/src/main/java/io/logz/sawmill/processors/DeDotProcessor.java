package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ProcessorProvider(type = "de_dot", factory = DeDotProcessor.Factory.class)
public class DeDotProcessor implements Processor {
    private String fieldSplit;
    private static final Logger logger = LoggerFactory.getLogger(DeDotProcessor.class);

    public DeDotProcessor(String fieldSplit) {
        this.fieldSplit = fieldSplit;
    }

    @Override
    public ProcessResult process(Doc doc) {
        Map<String,Object> docAsMap = doc.getSource();
        deDotMap(docAsMap);
        return ProcessResult.success();
    }

    private void deDotMap(Map<String, Object> docAsMap) {
        //Creating a copy of the Map in this level , and also in each level in the recursive level is required
        //in order to avoid ConcurrentModificationException , because in our case we want to replace the key thus actually we remove & add .
        //add is not supported while iterating , and the end result is to create a copy of the map.
        Map<String,Object> mapClone = new LinkedHashMap<>();
        deDotInner(docAsMap,mapClone);
        docAsMap.clear();
        docAsMap.putAll(mapClone);
    }

    private void deDotInner(Map<String, Object> docAsMap, Map<String,Object> mapClone) {
        Iterator<Map.Entry<String,Object>> iterator = docAsMap.entrySet().iterator();

        while(iterator.hasNext()){
            Map.Entry<String,Object> entry = iterator.next();
            String dedotedKey = dedot(entry.getKey());
            if(entry.getValue() instanceof Map){
                Map<String,Object> newInnerMap = new LinkedHashMap<>();
                mapClone.put(dedotedKey,newInnerMap);
                deDotInner((Map)entry.getValue(),newInnerMap);
            }
            else if(entry.getValue() instanceof List && !((List) entry.getValue()).isEmpty() && ((List) entry.getValue()).get(0) instanceof Map){
                List<Map<String,Object>> newInnerMapList = new ArrayList<>();
                mapClone.put(dedotedKey,newInnerMapList);
                ((List) entry.getValue()).stream().forEach(map ->
                {
                    Map<String,Object> newInnerMap = new LinkedHashMap<>();
                    newInnerMapList.add(newInnerMap);
                    deDotInner((Map<String, Object>) map,newInnerMap);
                });
            }else{
                mapClone.put(dedotedKey,entry.getValue());
            }
        }
    }

    private String dedot(String originalKey) {
        String dedotedKey = originalKey;
        if(dedotedKey.contains(".")){
            dedotedKey = dedotedKey.replace(".",fieldSplit);
        }
        return dedotedKey;
    }

    public static class Factory implements Processor.Factory {

        @Override
        public Processor create(Map<String,Object> config) {
            if(config!=null){
                DeDotProcessor.Configuration dedotConfiguration = null;
                try{
                    dedotConfiguration = JsonUtils.fromJsonMap(Configuration.class, config);
                }
                catch (Exception ex){
                    logger.error("failed to parse dedot processor confi",ex);
                    throw new ProcessorConfigurationException("failed to parse dedot processor config");
                }

                if(StringUtils.isNotEmpty(dedotConfiguration.getSeperator())){
                    return new DeDotProcessor(dedotConfiguration.getSeperator());
                }
            }
            return new DeDotProcessor(Configuration.DEDOT_DEFAULT_VAL);
        }
    }

    public static class Configuration implements Processor.Configuration {
        public final static String DEDOT_DEFAULT_VAL = "_";
        private String seperator;

        public Configuration() { }

        public Configuration(String seperator) {
            this.seperator = seperator;
        }
        public String getSeperator() {
            return seperator;
        }
    }
}