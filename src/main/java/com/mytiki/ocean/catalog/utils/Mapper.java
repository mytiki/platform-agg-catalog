/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.ocean.catalog.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.http.HttpStatusCode;

public class Mapper extends ObjectMapper {

    @Override
    public <T> T readValue(String src, Class<T> valueType){
        try {
            return super.readValue(src, valueType);
        } catch (JsonProcessingException e) {
            throw new ApiExceptionBuilder(HttpStatusCode.BAD_REQUEST)
                    .message("Bad Request")
                    .detail(e.getMessage())
                    .properties("raw", src)
                    .build();
        }
    }

    @Override
    public String writeValueAsString(Object value) {
        try {
            return super.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new ApiExceptionBuilder(HttpStatusCode.INTERNAL_SERVER_ERROR)
                    .message("Internal Server Error")
                    .detail(e.getMessage())
                    .build();
        }
    }
}
