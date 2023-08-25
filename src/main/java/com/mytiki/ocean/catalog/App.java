/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.ocean.catalog;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.mytiki.ocean.catalog.utils.ApiError;
import com.mytiki.ocean.catalog.utils.ApiException;
import com.mytiki.ocean.catalog.create.CreateHandler;
import com.mytiki.ocean.catalog.utils.Mapper;
import com.mytiki.ocean.catalog.utils.Router;


public class App implements RequestHandler<APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse> {

    public APIGatewayV2HTTPResponse handleRequest(final APIGatewayV2HTTPEvent request, final Context context) {
        APIGatewayV2HTTPEvent.RequestContext.Http http = request.getRequestContext().getHttp();
        try {
            return new Router<APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse>()
                    .add("POST", "/api/latest", new CreateHandler())
                    .handle(http.getMethod(), http.getPath(), request, context);
        }catch (ApiException e){
            return APIGatewayV2HTTPResponse.builder()
                    .withStatusCode(e.getStatus())
                    .withBody(new Mapper().writeValueAsString(e.getError()))
                    .build();
        }catch (Exception e){
            ApiError error = new ApiError();
            error.setMessage("Internal Server Error");
            error.setDetail(e.getMessage());
            return APIGatewayV2HTTPResponse.builder()
                    .withStatusCode(500)
                    .withBody(new Mapper().writeValueAsString(error))
                    .build();
        }
    }
}
