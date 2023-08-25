/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.ocean.catalog.mock;

import com.mytiki.ocean.catalog.create.CreateReq;
import com.mytiki.ocean.catalog.update.UpdateReq;
import com.mytiki.ocean.catalog.utils.Iceberg;
import org.apache.iceberg.catalog.TableIdentifier;

public class TableReq {

    public static CreateReq createReq() {
        CreateReq req = new CreateReq();
        req.setName(name);
        req.setSchema(raw);
        req.setPartition(partition);
        req.setIdentity(identity);
        return req;
    }

    public static UpdateReq updateReq() {
        UpdateReq req = new UpdateReq();
        req.setSchema(raw);
        req.setPartition(partition);
        req.setIdentity(identity);
        return req;
    }

    public static String name = "logs";
    public static String partition = "event_time";
    public static String identity = "level";
    public static String raw = """
            {
              "type": "record",
              "name": "Record",
              "fields": [
                {
                  "name": "level",
                  "type": "string"
                },
                {
                  "name": "event_time",
                  "type": {
                    "type": "long",
                    "logicalType": "timestamp-micros",
                    "adjust-to-utc": true
                  }
                },
                {
                  "name": "message",
                  "type": "string"
                },
                {
                  "name": "call_stack",
                  "type": {
                    "type": "array",
                    "items": "string"
                  }
                }
              ]
            }
            """;
}
