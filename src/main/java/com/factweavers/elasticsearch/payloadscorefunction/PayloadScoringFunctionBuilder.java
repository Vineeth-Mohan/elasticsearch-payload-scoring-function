package com.factweavers.elasticsearch.payloadscorefunction;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

public class PayloadScoringFunctionBuilder extends ScoreFunctionBuilder {
    private String field = null;
    private List<String> values = null;
    private String defaultValue = null;

    public PayloadScoringFunctionBuilder(String fieldName,List<String> values,String defaultValue) {
        this.field = fieldName;
        this.values = values;
        this.defaultValue = defaultValue;
    }

    @Override
    public String getName() {
        return PayloadScoringFunctionParser.NAMES[0];
    }


    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        if (field != null) {
            builder.field("field", field);
        }

        if (values != null) {
            builder.field("factor", values);
        }

        if (defaultValue != null) {
            builder.field("defaultValue", defaultValue);
        }

        builder.endObject();
    }
}
