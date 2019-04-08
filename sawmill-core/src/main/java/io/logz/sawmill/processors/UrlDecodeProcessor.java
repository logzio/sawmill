package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import io.logz.sawmill.exceptions.ProcessorExecutionException;
import io.logz.sawmill.utilities.JsonUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@ProcessorProvider(type = "urlDecode", factory = UrlDecodeProcessor.Factory.class)
public class UrlDecodeProcessor implements Processor {

    private Boolean allFields;
    private String charSet;
    private String field;
    private List<String> tagOnFailure;

    public UrlDecodeProcessor(Boolean allFields, String charSet, String field, List<String> tagOnFailure) {
        this.allFields = allFields;
        this.charSet = charSet;
        this.field = field;
        this.tagOnFailure = tagOnFailure;
    }

    @Override
    public ProcessResult process(Doc doc) throws InterruptedException {
        Map<String,Object> map = doc.getSource();
        try{
            if(allFields) {
                urlDecodeMap(map);
            }else if(doc.hasField(field)){
                doc.replaceFieldValue(field,decodeUrl(doc.getField(field)));
            }
        }
        catch(Exception ex){
            doc.appendList("tags", tagOnFailure);
            if(ex instanceof InterruptedException) throw new InterruptedException();
            return ProcessResult.failure("failed to url decode url", new ProcessorExecutionException("urlDecode", ex));
        }
        return ProcessResult.success();
    }

    private void urlDecodeMap(Map<String, Object> map) throws InterruptedException, UnsupportedEncodingException {

        for(Map.Entry<String, Object> entry: map.entrySet()){
            if(Thread.currentThread().isInterrupted()) throw new InterruptedException();
            if(entry.getValue() instanceof Map) urlDecodeMap((Map<String, Object>) entry.getValue());
            if(isListOfMaps(entry.getValue())) urlDecodeListOfMaps((List<Map<String, Object>>) entry.getValue());
            if(entry.getValue() instanceof String) entry.setValue(decodeUrl((String) entry.getValue()));
        }
    }

    private void urlDecodeListOfMaps(List<Map<String,Object>> listOfMaps) throws InterruptedException, UnsupportedEncodingException {
        for(Map<String,Object> currentInnerMap:listOfMaps){
            urlDecodeMap(currentInnerMap);
        }
    }

    private String decodeUrl(String valueToDecode) {
        String decodedUrl = valueToDecode;
        try{
            decodedUrl = URLDecoder.decode(valueToDecode,charSet);
        }
        catch (Exception ignored){}
        return decodedUrl;
    }

    private boolean isListOfMaps(Object object) {
        return object instanceof List && !((List) object).isEmpty() && ((List) object).get(0) instanceof Map;
    }

    public static class Factory implements Processor.Factory {

        @Override
        public Processor create(Map<String,Object> config) {
            UrlDecodeProcessor.Configuration urlDecodeConfiguration = JsonUtils.fromJsonMap(UrlDecodeProcessor.Configuration.class, config);
            if(!Charset.isSupported(urlDecodeConfiguration.getCharSet()))
            {
                throw new ProcessorConfigurationException("The given charSet is not Supported:"+urlDecodeConfiguration.getCharSet());
            }
            return new UrlDecodeProcessor(urlDecodeConfiguration.getAllFields(),urlDecodeConfiguration.getCharSet(),
                    urlDecodeConfiguration.getField(),urlDecodeConfiguration.getTagOnFailure());
        }
    }

    public static class Configuration implements Processor.Configuration {

        private boolean allFields=false;
        private String charSet ="UTF-8";
        private String field="message";
        private List<String> tagOnFailure = Collections.singletonList("_urldecodefailure");

        public Configuration() { }

        public boolean getAllFields() { return allFields; }
        public String getCharSet() { return charSet; }
        public String getField() { return field; }
        public List<String> getTagOnFailure() { return tagOnFailure; }
    }
}
