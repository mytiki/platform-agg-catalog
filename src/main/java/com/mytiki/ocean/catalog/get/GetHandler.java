/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.ocean.catalog.get;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.mytiki.ocean.catalog.utils.ApiExceptionBuilder;
import com.mytiki.ocean.catalog.utils.Iceberg;
import com.mytiki.ocean.catalog.utils.Mapper;
import com.mytiki.ocean.catalog.utils.Router;
import org.apache.avro.util.MapEntry;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import software.amazon.awssdk.http.HttpStatusCode;

import java.util.function.Function;
import java.util.stream.Collectors;

public class GetHandler implements RequestHandler<APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse> {

    @Override
    public APIGatewayV2HTTPResponse handleRequest(APIGatewayV2HTTPEvent request, Context context) {
        String name = Router.extract(
                request.getRequestContext().getHttp().getPath(),
                "(?<=/api/latest/)(\\S*[^/])");
        try {
            TableIdentifier identifier = TableIdentifier.of(Iceberg.database, name);
            Iceberg iceberg = new Iceberg();
            if (!iceberg.tableExists(identifier)) {
                throw new ApiExceptionBuilder(HttpStatusCode.BAD_REQUEST)
                        .message("Not Found")
                        .detail("Table does not exist")
                        .properties("name", name)
                        .build();
            }
            Table table = iceberg.loadTable(identifier);
            GetRsp body = new GetRsp();
            body.setName(name);
            body.setLocation(table.location());
            body.setSchema(table.schema().toString());
            body.setPartition(table.spec().fields().stream().collect(
                    Collectors.toMap((field) -> field.transform().toString(), PartitionField::name)));
            iceberg.close();
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
