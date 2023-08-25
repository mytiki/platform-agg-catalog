/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.ocean.catalog.utils;

import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Iceberg extends GlueCatalog {
    protected static final Logger logger = LogManager.getLogger();
    public static final Namespace database = Namespace.of("ocean");
    public static final String warehouse = "s3://mytiki-ocean";

    public Iceberg() {
        super();
        Map<String, String> props = new HashMap<>() {{
            put("catalog-name", "iceberg");
            put("catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog");
            put("warehouse", warehouse);
            put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            put("glue.skip-archive", "true");
        }};
        super.initialize("glue_catalog", props);
    }

    @Override
    public void close() {
        try {
            super.close();
        } catch (IOException e) {
            logger.error(e);
        }
    }
}
