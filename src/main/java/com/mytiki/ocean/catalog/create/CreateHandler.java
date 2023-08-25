/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.ocean.catalog.create;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.mytiki.ocean.catalog.utils.Iceberg;
import com.mytiki.ocean.catalog.utils.Mapper;
import com.mytiki.ocean.catalog.utils.ApiExceptionBuilder;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.http.HttpStatusCode;

public class CreateHandler implements RequestHandler<APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse> {
    private final Mapper mapper = new Mapper();

    @Override
    public APIGatewayV2HTTPResponse handleRequest(APIGatewayV2HTTPEvent request, Context context) {
        Iceberg iceberg = new Iceberg();
        CreateReq req = mapper.readValue(request.getBody(), CreateReq.class);
        try{
            TableIdentifier identifier = TableIdentifier.of(Iceberg.database, req.getName());
            Schema schema = new Schema.Parser().parse(req.getSchema());
            PartitionSpec spec = PartitionSpec.builderFor(AvroSchemaUtil.toIceberg(schema))
                    .hour(req.getPartition())
                    .identity(req.getIdentity())
                    .build();
            if(iceberg.tableExists(identifier)){
                throw new ApiExceptionBuilder(HttpStatusCode.BAD_REQUEST)
                        .message("Bad Request")
                        .detail("Table already exists")
                        .properties("name", req.getName())
                        .build();
            }
            Table table = iceberg.createTable(identifier, AvroSchemaUtil.toIceberg(schema), spec);
            iceberg.close();
            CreateRsp body = new CreateRsp();
            body.setName(table.name());
            body.setLocation(table.location());
            return APIGatewayV2HTTPResponse.builder()
                    .withStatusCode(HttpStatusCode.OK)
                    .withBody(mapper.writeValueAsString(body))
                    .build();
        }catch (SchemaParseException | IllegalArgumentException ex) {
            throw new ApiExceptionBuilder(HttpStatusCode.BAD_REQUEST)
                    .message("Bad Request")
                    .detail(ex.getMessage())
                    .properties("schema", req.getSchema())
                    .build();
        }
    }
}
