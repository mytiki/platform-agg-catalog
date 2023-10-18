/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.core.iceberg.catalog.delete;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.mytiki.core.iceberg.utils.ApiExceptionBuilder;
import com.mytiki.core.iceberg.utils.Iceberg;
import com.mytiki.core.iceberg.utils.Mapper;
import com.mytiki.core.iceberg.utils.Router;
import org.apache.iceberg.catalog.TableIdentifier;
import software.amazon.awssdk.http.HttpStatusCode;

public class DeleteHandler implements RequestHandler<APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse> {
    private final Iceberg iceberg;

    public DeleteHandler(Iceberg iceberg) {
        super();
        this.iceberg = iceberg;
    }

    @Override
    public APIGatewayV2HTTPResponse handleRequest(APIGatewayV2HTTPEvent request, Context context) {
        String name = Router.extract(
                request.getRequestContext().getHttp().getPath(),
                "(?<=/api/latest/)(\\S*[^/])");
        try {
            TableIdentifier identifier = TableIdentifier.of(iceberg.getDatabase(), name);
            if (!iceberg.tableExists(identifier)) {
                throw new ApiExceptionBuilder(HttpStatusCode.NOT_FOUND)
                        .message("Not Found")
                        .detail("Table does not exist")
                        .properties("name", name)
                        .build();
            }
            if (!iceberg.dropTable(identifier)) {
                throw new ApiExceptionBuilder(HttpStatusCode.INTERNAL_SERVER_ERROR)
                        .message("Delete Failed")
                        .properties("name", name)
                        .build();
            }
            DeleteRsp body = new DeleteRsp();
            body.setName(name);
            return APIGatewayV2HTTPResponse.builder()
                    .withStatusCode(HttpStatusCode.OK)
                    .withBody(new Mapper().writeValueAsString(body))
                    .build();
        }catch (IllegalArgumentException ex) {
            throw new ApiExceptionBuilder(HttpStatusCode.BAD_REQUEST)
                    .message("Bad Request")
                    .detail(ex.getMessage())
                    .build();
        }
    }
}
