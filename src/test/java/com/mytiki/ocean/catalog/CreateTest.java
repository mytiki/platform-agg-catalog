/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.ocean.catalog;

import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.mytiki.ocean.catalog.create.CreateHandler;
import com.mytiki.ocean.catalog.create.CreateReq;
import com.mytiki.ocean.catalog.create.CreateRsp;
import com.mytiki.ocean.catalog.mock.MockIceberg;
import com.mytiki.ocean.catalog.mock.MockReq;
import com.mytiki.ocean.catalog.utils.ApiException;
import com.mytiki.ocean.catalog.utils.Iceberg;
import com.mytiki.ocean.catalog.utils.Mapper;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@RunWith(MockitoJUnitRunner.class)
public class CreateTest {

    @Mock
    Iceberg iceberg;
    MockIceberg mockIceberg;

    @Before
    public void init() {
        mockIceberg = new MockIceberg(iceberg);
    }

    @Test
    public void HandleRequest_New_200() {
        CreateReq body = MockReq.createReq();
        APIGatewayV2HTTPEvent request = APIGatewayV2HTTPEvent.builder()
                .withBody(new Mapper().writeValueAsString(body))
                .withRequestContext(APIGatewayV2HTTPEvent.RequestContext.builder()
                        .withHttp(APIGatewayV2HTTPEvent.RequestContext.Http.builder()
                                .withMethod("POST")
                                .withPath("/api/latest")
                                .build())
                        .build())
                .build();
        APIGatewayV2HTTPResponse response = new CreateHandler(iceberg).handleRequest(request, null);
        assertEquals(200, response.getStatusCode());
        CreateRsp res = new Mapper().readValue(response.getBody(), CreateRsp.class);
        assertEquals(mockIceberg.getName(), res.getName());
        assertEquals(mockIceberg.getLocation(), res.getLocation());
    }

    @Test
    public void HandleRequest_Exists_400() {
        mockIceberg.setTableExists(true);
        CreateReq body = MockReq.createReq();
        APIGatewayV2HTTPEvent request = APIGatewayV2HTTPEvent.builder()
                .withBody(new Mapper().writeValueAsString(body))
                .withRequestContext(APIGatewayV2HTTPEvent.RequestContext.builder()
                        .withHttp(APIGatewayV2HTTPEvent.RequestContext.Http.builder()
                                .withMethod("POST")
                                .withPath("/api/latest")
                                .build())
                        .build())
                .build();
        ApiException exception = assertThrows(ApiException.class, () -> {
            new CreateHandler(iceberg).handleRequest(request, null);
        });
        assertEquals(400, exception.getStatus());
    }
}
