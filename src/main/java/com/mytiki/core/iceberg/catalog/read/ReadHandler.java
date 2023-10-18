/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.core.iceberg.catalog.read;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.mytiki.core.iceberg.utils.*;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.http.HttpStatusCode;

import java.util.stream.Collectors;

public class ReadHandler implements RequestHandler<APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse> {
    protected static final Logger logger = Initialize.logger(ReadHandler.class);
    private final Iceberg iceberg;

    public ReadHandler(Iceberg iceberg) {
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
            Table table = iceberg.loadTable(identifier);
            ReadRsp body = new ReadRsp();
            body.setName(name);
            body.setLocation(table.location());
            body.setSchema(table.schema().toString());
            body.setPartition(table.spec().fields().stream().collect(
                    Collectors.toMap((field) -> field.transform().toString(), PartitionField::name)));
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
