package io.logz.sawmill;

import com.github.mustachejava.TemplateFunction;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateTemplateHandler {

    public TemplateFunction date() {
        return this::getCurrentDateByFormat;
    }

    private String getCurrentDateByFormat(String dateFormat) {
        return new SimpleDateFormat(dateFormat).format(new Date());
    }
}
