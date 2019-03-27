package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import org.apache.commons.lang3.StringUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DeDotProcessor implements Processor {
    private String fieldSplit="_";

    public DeDotProcessor(String fieldSplit) {
        this.fieldSplit = !StringUtils.isEmpty(fieldSplit) ? fieldSplit:this.fieldSplit ;
    }

    @Override
    public ProcessResult process(Doc doc) throws InterruptedException {
        Map<String,Object> docAsMap = doc.getSource();
        deDotMap(docAsMap);
        return ProcessResult.success();
    }

    private void deDotMap(Map<String, Object> docAsMap) {
        Iterator<Map.Entry<String,Object>> iterator = docAsMap.entrySet().iterator();
        while(iterator.hasNext()){
            Map.Entry<String,Object> entry = iterator.next();
            if(entry.getValue() instanceof Map){
                deDotMap((Map)entry.getValue());
            }
            else if(entry.getValue() instanceof List && !((List) entry.getValue()).isEmpty() && ((List) entry.getValue()).get(0) instanceof Map){
                ((List) entry.getValue()).stream().forEach(map -> deDotMap((Map<String, Object>) map));
            }
            String originalKey = entry.getKey();
            if(originalKey.contains(".")){
                Object valueBackup = entry.getValue();
                iterator.remove();
                String newKey = originalKey.replace(".",fieldSplit);
                docAsMap.put(newKey,valueBackup);
            }
        }
    }
}