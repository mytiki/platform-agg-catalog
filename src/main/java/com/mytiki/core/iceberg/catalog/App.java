/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.core.iceberg.catalog;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.mytiki.core.iceberg.catalog.create.CreateHandler;
import com.mytiki.core.iceberg.catalog.delete.DeleteHandler;
import com.mytiki.core.iceberg.catalog.read.ReadHandler;
import com.mytiki.core.iceberg.utils.*;
import org.apache.log4j.Logger;

public class App implements RequestHandler<APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse> {
    protected static final Logger logger = Logger.getLogger(App.class);

    public APIGatewayV2HTTPResponse handleRequest(final APIGatewayV2HTTPEvent request, final Context context) {
        Initialize.logger();
        APIGatewayV2HTTPEvent.RequestContext.Http http = request.getRequestContext().getHttp();
        try {
            Iceberg iceberg = Iceberg.load();
            APIGatewayV2HTTPResponse response = new Router<APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse>()
                    .add("POST", "/api/latest/?", new CreateHandler(iceberg))
                    .add("DELETE", "/api/latest/.*", new DeleteHandler(iceberg))
                    //.add("POST", "/api/latest/.*", new UpdateHandler(iceberg))
                    .add("GET", "/api/latest/.*", new ReadHandler(iceberg))
                    .handle(http.getMethod(), http.getPath(), request, context);
            iceberg.close();
            return response;
        }catch (ApiException e){
            logger.debug(e.getMessage());
            return APIGatewayV2HTTPResponse.builder()
                    .withStatusCode(e.getStatus())
                    .withBody(new Mapper().writeValueAsString(e.getError()))
                    .build();
        }catch (Exception e){
            ApiError error = new ApiError();
            error.setMessage("Internal Server Error");
            error.setDetail(e.getMessage());
            logger.error(e.getMessage(), e.fillInStackTrace());
            return APIGatewayV2HTTPResponse.builder()
                    .withStatusCode(500)
                    .withBody(new Mapper().writeValueAsString(error))
                    .build();
        }
    }
}
