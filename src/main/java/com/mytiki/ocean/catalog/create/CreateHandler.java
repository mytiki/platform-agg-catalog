/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.ocean.catalog.create;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.mytiki.ocean.catalog.utils.ApiExceptionBuilder;
import com.mytiki.ocean.catalog.utils.Iceberg;
import com.mytiki.ocean.catalog.utils.Mapper;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.log4j.Logger;
import software.amazon.awssdk.http.HttpStatusCode;

import java.time.Instant;

public class CreateHandler implements RequestHandler<APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse> {
    protected static final Logger logger = Logger.getLogger(CreateHandler.class);
    private final Mapper mapper = new Mapper();
    private final Iceberg iceberg;

    public CreateHandler(Iceberg iceberg) {
        super();
        this.iceberg = iceberg;
    }

    @Override
    public APIGatewayV2HTTPResponse handleRequest(APIGatewayV2HTTPEvent request, Context context) {
        CreateReq req = mapper.readValue(request.getBody(), CreateReq.class);
        try{
            TableIdentifier identifier = TableIdentifier.of(iceberg.getDatabase(), req.getName());
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
            String location =  String.join("", iceberg.getWarehouse(), "/", req.getName(),
                    "_", String.valueOf(Instant.now().toEpochMilli()));
            Table table = iceberg.createTable(identifier, AvroSchemaUtil.toIceberg(schema), spec,
                    location, null);
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
                    .build();
        }
    }
}
