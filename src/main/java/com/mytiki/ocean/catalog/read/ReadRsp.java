/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.ocean.catalog.read;

import java.util.Map;

public class ReadRsp {
    private String name;
    private String location;
    private String schema;
    private Map<String, String> partition;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public Map<String, String> getPartition() {
        return partition;
    }

    public void setPartition(Map<String, String> partition) {
        this.partition = partition;
    }
}
