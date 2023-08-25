/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.ocean.catalog.update;

public class UpdateRsp {
    private String name;
    private String location;
    private String archivedTo;

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

    public String getArchivedTo() {
        return archivedTo;
    }

    public void setArchivedTo(String archivedTo) {
        this.archivedTo = archivedTo;
    }
}
