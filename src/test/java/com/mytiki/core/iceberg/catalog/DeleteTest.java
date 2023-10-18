/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.core.iceberg.catalog;


import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.amazonaws.services.lambda.runtime.tests.annotations.Event;
import com.mytiki.core.iceberg.catalog.delete.DeleteHandler;
import com.mytiki.core.iceberg.catalog.delete.DeleteRsp;
import com.mytiki.core.iceberg.catalog.mock.MockIceberg;
import com.mytiki.core.iceberg.catalog.read.ReadHandler;
import com.mytiki.core.iceberg.catalog.read.ReadRsp;
import com.mytiki.core.iceberg.utils.ApiException;
import com.mytiki.core.iceberg.utils.Mapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import software.amazon.awssdk.http.HttpStatusCode;

public class DeleteTest {

    MockIceberg mockIceberg;

    @BeforeEach
    public void init() {
        mockIceberg = new MockIceberg();
    }

    @ParameterizedTest
    @Event(value = "events/http_event_delete.json", type = APIGatewayV2HTTPEvent.class)
    public void HandleRequest_Exists_200(APIGatewayV2HTTPEvent event) {
        mockIceberg.setTableExists(true);
        APIGatewayV2HTTPResponse response = new DeleteHandler(mockIceberg.iceberg()).handleRequest(event, null);
        Assertions.assertEquals(HttpStatusCode.OK, response.getStatusCode());
        DeleteRsp res = new Mapper().readValue(response.getBody(), DeleteRsp.class);
        Assertions.assertEquals(res.getName(), mockIceberg.getName());
    }

    @ParameterizedTest
    @Event(value = "events/http_event_delete.json", type = APIGatewayV2HTTPEvent.class)
    public void HandleRequest_NoTable_404(APIGatewayV2HTTPEvent event) {
        mockIceberg.setTableExists(false);
        ApiException exception = Assertions.assertThrows(ApiException.class, () -> {
            new DeleteHandler(mockIceberg.iceberg()).handleRequest(event, null);
        });
        Assertions.assertEquals(HttpStatusCode.NOT_FOUND, exception.getStatus());
    }
}
