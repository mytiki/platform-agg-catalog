/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.core.iceberg.catalog;

import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.amazonaws.services.lambda.runtime.tests.annotations.Event;
import com.mytiki.core.iceberg.catalog.create.CreateHandler;
import com.mytiki.core.iceberg.catalog.create.CreateRsp;
import com.mytiki.core.iceberg.catalog.mock.MockIceberg;
import com.mytiki.core.iceberg.utils.ApiException;
import com.mytiki.core.iceberg.utils.Mapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import software.amazon.awssdk.http.HttpStatusCode;


public class CreateTest {

    MockIceberg mockIceberg;

    @BeforeEach
    public void init() {
        mockIceberg = new MockIceberg();
    }


    @ParameterizedTest
    @Event(value = "events/http_event_post.json", type = APIGatewayV2HTTPEvent.class)
    public void HandleRequest_New_200(APIGatewayV2HTTPEvent event) {
        APIGatewayV2HTTPResponse response = new CreateHandler(mockIceberg.iceberg()).handleRequest(event, null);
        Assertions.assertEquals(HttpStatusCode.OK, response.getStatusCode());
        CreateRsp res = new Mapper().readValue(response.getBody(), CreateRsp.class);
        Assertions.assertEquals(res.getName(), mockIceberg.getName());
        Assertions.assertEquals(res.getLocation(), mockIceberg.getLocation());
    }

    @ParameterizedTest
    @Event(value = "events/http_event_post.json", type = APIGatewayV2HTTPEvent.class)
    public void HandleRequest_Exists_400(APIGatewayV2HTTPEvent event) {
        mockIceberg.setTableExists(true);
        ApiException exception = Assertions.assertThrows(ApiException.class, () -> {
            new CreateHandler(mockIceberg.iceberg()).handleRequest(event, null);
        });
        Assertions.assertEquals(HttpStatusCode.BAD_REQUEST, exception.getStatus());
    }
}
