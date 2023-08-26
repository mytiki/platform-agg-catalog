/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.ocean.catalog.mock;


import com.mytiki.ocean.common.Iceberg;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

public class MockIceberg {
    private final Iceberg iceberg;
    private final String location;
    private final String name;
    private final String schema = "-----MOCKED_SCHEMA-----";

    public MockIceberg(Iceberg iceberg) {
        this.iceberg = iceberg;
        try (InputStream input = Iceberg.class.getClassLoader().getResourceAsStream("iceberg.properties")) {
            Properties prop = new Properties();
            prop.load(input);
            Table table = Mockito.mock(Table.class);
            PartitionSpec spec = Mockito.mock(PartitionSpec.class);
            Schema schema = Mockito.mock(Schema.class);
            location = prop.getProperty("warehouse");
            name = prop.getProperty("database-name");
            Mockito.lenient().doReturn(location).when(iceberg).getWarehouse();
            Mockito.lenient().doReturn(Namespace.of(name)).when(iceberg).getDatabase();
            Mockito.lenient().doNothing().when(iceberg).close();
            Mockito.lenient().doNothing().when(iceberg).renameTable(Mockito.any(), Mockito.any());
            Mockito.lenient().doReturn(this.schema).when(schema).toString();
            Mockito.lenient().doReturn(List.of()).when(spec).fields();
            Mockito.lenient().doReturn(schema).when(table).schema();
            Mockito.lenient().doReturn(spec).when(table).spec();
            Mockito.lenient().doReturn(location).when(table).location();
            Mockito.lenient().doReturn(name).when(table).name();
            Mockito.lenient().doReturn(table).when(iceberg).loadTable(Mockito.any());
            Mockito.lenient().doReturn(table).when(iceberg)
                    .createTable(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
            Mockito.lenient().doReturn(false).when(iceberg).tableExists(Mockito.any());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public String getLocation() {
        return location;
    }

    public String getName() {
        return name;
    }

    public String getSchema() {
        return schema;
    }

    public void setTableExists(boolean exists) {
        Mockito.doReturn(exists).when(iceberg).tableExists(Mockito.any());
    }
}
