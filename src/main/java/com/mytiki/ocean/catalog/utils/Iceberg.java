/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.ocean.catalog.utils;

import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Iceberg extends GlueCatalog {
    protected static final Logger logger = LogManager.getLogger();

    private final String warehouse;
    private final Namespace database;

    public Iceberg() {
        super();
        try (InputStream input = Iceberg.class.getClassLoader().getResourceAsStream("iceberg.properties")) {
            Properties prop = new Properties();
            prop.load(input);
            Map<String, String> props = new HashMap<>() {{
                put("catalog-name", prop.getProperty("catalog-name"));
                put("catalog-impl", prop.getProperty("catalog-impl"));
                put("warehouse", prop.getProperty("warehouse"));
                put("io-impl", prop.getProperty("io-impl"));
                put("glue.skip-archive", prop.getProperty("glue.skip-archive"));
            }};
            warehouse = prop.getProperty("warehouse");
            database = Namespace.of(prop.getProperty("database-name"));
            super.initialize("glue_catalog", props);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() {
        try {
            super.close();
        } catch (IOException e) {
            logger.error(e);
        }
    }

    public String getWarehouse() {
        return warehouse;
    }

    public Namespace getDatabase() {
        return database;
    }
}
