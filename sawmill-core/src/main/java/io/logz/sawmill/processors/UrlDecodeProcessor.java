package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import io.logz.sawmill.utilities.JsonUtils;

import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

@ProcessorProvider(type = "urlDecode", factory = UrlDecodeProcessor.Factory.class)
public class UrlDecodeProcessor implements Processor {

    private boolean allFields;
    private String charSet;
    private String field;

    public UrlDecodeProcessor(Boolean allFields, String charSet, String field) {
        this.allFields = allFields;
        this.charSet = charSet;
        this.field = field;
    }

    @Override
    public ProcessResult process(Doc doc) throws InterruptedException {
        Map<String,Object> map = doc.getSource();
        if(allFields) {
            urlDecodeMap(map);
        }else if(doc.hasField(field)){
            doc.replaceFieldValue(field,decodeUrl(doc.getField(field)));
        }else{
            return ProcessResult.failure(String.format("failed to decode field [%s], field is missing", field));
        }
        return ProcessResult.success();
    }

    private Map<String,Object> urlDecodeMap(Map<String, Object> map) throws InterruptedException {

        for(Map.Entry<String, Object> entry: map.entrySet()){
            entry.setValue(urlDecodeObject(entry.getValue()));
        }
        return map;
    }

    private Object urlDecodeObject(Object value) throws InterruptedException {
        if(Thread.currentThread().isInterrupted()) throw new InterruptedException();
        if(value instanceof String) value = decodeUrl((String) value);
        else if(value instanceof Map)  urlDecodeMap((Map<String, Object>) value);
        else if(value instanceof List) {
          for(int i=0;i<((List) value).size();i++){
              ((List) value).set(i, urlDecodeObject(((List) value).get(i)));
          }
        }
        return value;
    }

    private String decodeUrl(String valueToDecode) {
        return decodeURIComponent(valueToDecode, charSet);
    }

    public static String decodeURIComponent(String valueToDecode, String charSet) {
        String decodedUrl = valueToDecode;
        try{
            decodedUrl = URLDecoder.decode(valueToDecode.replace("+","%2B"),charSet);
        }
        catch (Exception ignored){}
        return decodedUrl;
    }

    public static class Factory implements Processor.Factory {

        @Override
        public Processor create(Map<String,Object> config) {
            UrlDecodeProcessor.Configuration urlDecodeConfiguration = JsonUtils.fromJsonMap(UrlDecodeProcessor.Configuration.class, config);
            if(!Charset.isSupported(urlDecodeConfiguration.getCharset()))
            {
                throw new ProcessorConfigurationException("The given charset is not Supported:"+urlDecodeConfiguration.getCharset());
            }
            return new UrlDecodeProcessor(urlDecodeConfiguration.getAllFields(),urlDecodeConfiguration.getCharset(),
                    urlDecodeConfiguration.getField());
        }
    }

    public static class Configuration implements Processor.Configuration {

        private boolean allFields=false;
        private String charset ="UTF-8";
        private String field="message";

        public Configuration() { }

        public boolean getAllFields() { return allFields; }
        public String getCharset() { return charset; }
        public String getField() { return field; }
    }
}
