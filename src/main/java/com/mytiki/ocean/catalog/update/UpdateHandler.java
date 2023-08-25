/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.ocean.catalog.update;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.mytiki.ocean.catalog.create.CreateReq;
import com.mytiki.ocean.catalog.drop.DropRsp;
import com.mytiki.ocean.catalog.utils.ApiExceptionBuilder;
import com.mytiki.ocean.catalog.utils.Iceberg;
import com.mytiki.ocean.catalog.utils.Mapper;
import com.mytiki.ocean.catalog.utils.Router;
import org.apache.avro.Schema;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import software.amazon.awssdk.http.HttpStatusCode;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.UUID;

public class UpdateHandler implements RequestHandler<APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse> {
    private final Mapper mapper = new Mapper();

    @Override
    public APIGatewayV2HTTPResponse handleRequest(APIGatewayV2HTTPEvent request, Context context) {
        String name = Router.extract(
                request.getRequestContext().getHttp().getPath(),
                "(?<=/api/latest/)(\\S*[^/])");
        UpdateReq req = mapper.readValue(request.getBody(), UpdateReq.class);
        try {
            String archiveName = name + "_archive_" + Instant.now().toEpochMilli();
            TableIdentifier identifier = TableIdentifier.of(Iceberg.database, name);
            TableIdentifier archiveIdentifier = TableIdentifier.of(Iceberg.database, archiveName);
            Schema schema = new Schema.Parser().parse(req.getSchema());
            PartitionSpec spec = PartitionSpec.builderFor(AvroSchemaUtil.toIceberg(schema))
                    .hour(req.getPartition())
                    .identity(req.getIdentity())
                    .build();
            Iceberg iceberg = new Iceberg();
            if (!iceberg.tableExists(identifier)) {
                throw new ApiExceptionBuilder(HttpStatusCode.BAD_REQUEST)
                        .message("Bad Request")
                        .detail("Table does not exist")
                        .properties("name", name)
                        .build();
            }
            iceberg.renameTable(identifier, archiveIdentifier);
            String location =  String.join("",
                    Iceberg.warehouse, "/", name, "_", String.valueOf(Instant.now().toEpochMilli()));
            Table table = iceberg.createTable(identifier, AvroSchemaUtil.toIceberg(schema), spec, location, null);

            UpdateRsp body = new UpdateRsp();
            body.setName(name);
            body.setLocation(table.location());
            body.setArchivedTo(archiveName);
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
