/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.ocean.catalog;

import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.mytiki.ocean.catalog.create.CreateReq;
import com.mytiki.ocean.catalog.utils.Mapper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AppTest {

    private final String raw = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"Record\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"level\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"event_time\",\n" +
            "      \"type\": {\n" +
            "        \"type\": \"long\",\n" +
            "        \"logicalType\": \"timestamp-micros\",\n" +
            "        \"adjust-to-utc\": true\n" +
            "      }\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"message\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"call_stack\",\n" +
            "      \"type\": {\n" +
            "        \"type\": \"array\",\n" +
            "        \"items\": \"string\"\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}\n";

    @Test
    public void success() {
        App app = new App();

//        CreateReq body = new CreateReq();
//        body.setSchema(raw);
//        body.setName("logs");
//        body.setIdentity("level");
//        body.setPartition("event_time");

        APIGatewayV2HTTPEvent request = APIGatewayV2HTTPEvent.builder()
                //.withBody(new Mapper().writeValueAsString(body))
                .withRequestContext(APIGatewayV2HTTPEvent.RequestContext.builder()
                        .withHttp(APIGatewayV2HTTPEvent.RequestContext.Http.builder()
                                .withMethod("DELETE")
                                .withPath("/api/latest/logs")
                                .build())
                        .build())
                .build();
        APIGatewayV2HTTPResponse response = app.handleRequest(request, null);
        assertEquals(200, response.getStatusCode());
    }
}
