/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.ocean.catalog;

import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class AppTest {

    @Test
    public void success() {
        App app = new App();
        String body = UUID.randomUUID().toString();
        APIGatewayV2HTTPEvent request = APIGatewayV2HTTPEvent.builder()
                .withBody(body)
                .build();
        APIGatewayV2HTTPResponse response = app.handleRequest(request, null);
        assertEquals(200, response.getStatusCode());
        assertEquals(body, response.getBody());
    }
}
