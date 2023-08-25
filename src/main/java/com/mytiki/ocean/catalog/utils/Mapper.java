/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.ocean.catalog.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Mapper extends ObjectMapper {

    @Override
    public <T> T readValue(String src, Class<T> valueType){
        try {
            return super.readValue(src, valueType);
        } catch (JsonProcessingException e) {
            throw new ApiExceptionBuilder(400)
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
            throw new ApiExceptionBuilder(500)
                    .message("Internal Server Error")
                    .detail(e.getMessage())
                    .build();
        }
    }
}
