/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.ocean.catalog;

import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.mytiki.ocean.catalog.create.CreateHandler;
import com.mytiki.ocean.catalog.create.CreateReq;
import com.mytiki.ocean.catalog.mock.TableReq;
import com.mytiki.ocean.catalog.utils.ApiException;
import com.mytiki.ocean.catalog.utils.Iceberg;
import com.mytiki.ocean.catalog.utils.Mapper;
import org.apache.hadoop.thirdparty.protobuf.Api;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@RunWith(MockitoJUnitRunner.class)
public class CreateTest {

    @Spy
    Iceberg iceberg = new Iceberg();

    @Test
    @Ignore
    public void HandleRequest_New_200() {
        //TODO
    }

    @Test
    public void HandleRequest_Exists_400() {
        Mockito.doReturn(true).when(iceberg).tableExists(Mockito.any());
        CreateReq body = TableReq.createReq();
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
