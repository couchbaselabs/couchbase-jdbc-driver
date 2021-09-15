/*
 * Copyright (c) 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.jdbc.analytics;

import com.couchbase.client.core.json.Mapper;

import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AnalyticsMetaDataQueries {

  private static final String SCHEMALESS = "SCHEMALESS";
  private static final String TABLE = "TABLE";
  private static final String VIEW = "VIEW";

  private AnalyticsMetaDataQueries() {}

  static String catalogsQuery() {
    return "select TABLE_CAT " +
      "from Metadata.`Dataverse` " +
      "let name = decode_dataverse_name(DataverseName), " +
      "TABLE_CAT = name[0] " +
      "where array_length(name) between 1 and 2 " +
      "group by TABLE_CAT " +
      "order by TABLE_CAT";
  }

  static String schemasQuery(String catalog, String schemaPattern) {
    StringBuilder sql = new StringBuilder(512);
    sql.append("select TABLE_SCHEM, TABLE_CATALOG ");
    sql.append("from Metadata.`Dataverse` ");
    sql.append("let name = decode_dataverse_name(DataverseName), ");
    sql.append("TABLE_CATALOG = name[0], ");
    sql.append("TABLE_SCHEM = case array_length(name) when 1 then null else name[1] end ");
    sql.append("where array_length(name) between 1 and 2 ");
    if (catalog != null) {
      sql.append("and TABLE_CATALOG = \"").append(catalog).append("\" ");
    }
    if (schemaPattern != null) {
      sql.append("and if_null(TABLE_SCHEM, '') like \"").append(schemaPattern).append("\" ");
    }
    sql.append("order by TABLE_CATALOG, TABLE_SCHEM");
    return sql.toString();
  }

  static String tablesQuery(String catalog, String schemaPattern, String tableNamePattern, String[] types) {
    String datasetTermTabular = getDatasetTerm(true);
    String datasetTermNonTabular = getDatasetTerm(false);
    String viewTermTabular = getViewTerm(true);
    String viewTermNonTabular = getViewTerm(false);

    StringBuilder sql = new StringBuilder(1024);
    sql.append("select TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, null REMARKS, null TYPE_CAT, ");
    sql.append("null TYPE_SCHEM, null TYPE_NAME, null SELF_REFERENCING_COL_NAME, null REF_GENERATION ");
    sql.append("from Metadata.`Dataset` ds join Metadata.`Datatype` dt ");
    sql.append("on ds.DatatypeDataverseName = dt.DataverseName and ds.DatatypeName = dt.DatatypeName ");
    sql.append("let dvname = decode_dataverse_name(ds.DataverseName), ");
    sql.append("isDataset = (ds.DatasetType = 'INTERNAL' or ds.DatasetType = 'EXTERNAL'), ");
    sql.append("isView = ds.DatasetType = 'VIEW', ");
    sql.append("hasFields = array_length(dt.Derived.Record.Fields) > 0, ");
    sql.append("TABLE_CAT = dvname[0], ");
    sql.append("TABLE_SCHEM = case array_length(dvname) when 1 then null else dvname[1] end, ");
    sql.append("TABLE_NAME = ds.DatasetName, ");
    sql.append("TABLE_TYPE = case ");
    sql.append("when isDataset then (case when hasFields then '").append(datasetTermTabular).append("' else '")
      .append(datasetTermNonTabular).append("' end) ");
    sql.append("when isView then (case when hasFields then '").append(viewTermTabular).append("' else '")
      .append(viewTermNonTabular).append("' end) ");
    sql.append("else null end ");
    sql.append("where array_length(dvname) between 1 and 2 ");
    if (catalog != null) {
      sql.append("and TABLE_CAT = \"").append(catalog).append("\" ");
    }
    if (schemaPattern != null) {
      sql.append("and if_null(TABLE_SCHEM, '') like \"").append(schemaPattern).append("\" ");
    }
    if (tableNamePattern != null) {
      sql.append("and TABLE_NAME like \"").append(tableNamePattern).append("\" ");
    }
    sql.append("and TABLE_TYPE ").append(types != null ? "in " + Mapper.encodeAsString(types) : "is not null").append(" ");
    sql.append("order by TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME");

    return sql.toString();
  }

  static String columnsQuery(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) {
    StringBuilder sql = new StringBuilder(2048);
    sql.append("select TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, DATA_TYPE, TYPE_NAME, COLUMN_SIZE, ");
    sql.append("1 BUFFER_LENGTH, null DECIMAL_DIGITS, 2 NUM_PREC_RADIX, NULLABLE, ");
    sql.append("null REMARKS, null COLUMN_DEF, DATA_TYPE SQL_DATA_TYPE,");
    sql.append("0 SQL_DATETIME_SUB, COLUMN_SIZE CHAR_OCTET_LENGTH, ORDINAL_POSITION, ");
    sql.append("case NULLABLE when 0 then 'NO' else 'YES' end IS_NULLABLE, ");
    sql.append("null SCOPE_CATALOG, null SCOPE_SCHEMA, null SCOPE_TABLE, null SOURCE_DATA_TYPE, ");
    sql.append("'NO' IS_AUTOINCREMENT, 'NO' IS_GENERATEDCOLUMN ");
    sql.append("from Metadata.`Dataset` ds ");
    sql.append("join Metadata.`Datatype` dt ");
    sql.append("on ds.DatatypeDataverseName = dt.DataverseName and ds.DatatypeName = dt.DatatypeName ");
    sql.append("unnest dt.Derived.Record.Fields as field at fieldpos ");
    sql.append("left join Metadata.`Datatype` dt2 ");
    sql.append(
      "on field.FieldType = dt2.DatatypeName and ds.DataverseName = dt2.DataverseName and dt2.Derived is known ");
    sql.append("let dvname = decode_dataverse_name(ds.DataverseName), ");
    sql.append("TABLE_CAT = dvname[0], ");
    sql.append("TABLE_SCHEM = case array_length(dvname) when 1 then null else dvname[1] end, ");
    sql.append("TABLE_NAME = ds.DatasetName, ");
    sql.append("COLUMN_NAME = field.FieldName, ");
    sql.append("TYPE_NAME = case ");
    for (AnalyticsDataType nestedType : new AnalyticsDataType[] { AnalyticsDataType.OBJECT, AnalyticsDataType.ARRAY,
      AnalyticsDataType.MULTISET }) {
      sql.append(String.format("when dt2.Derived.%s is known then '%s' ",
        AnalyticsDataType.getDerivedRecordName(nestedType), nestedType.getTypeName()));
    }
    sql.append("else field.FieldType end, ");
    sql.append("DATA_TYPE = ");
    sql.append("case TYPE_NAME ");
    for (AnalyticsDataType type : AnalyticsDataType.values()) {
      JDBCType jdbcType = type.getJdbcType();
      if (type.isNullOrMissing() || jdbcType.equals(JDBCType.OTHER)) {
        // will be handled by the 'else' clause
        continue;
      }
      sql.append("when '").append(type.getTypeName()).append("' ");
      sql.append("then ").append(jdbcType.getVendorTypeNumber()).append(" ");
    }
    sql.append("else ").append(JDBCType.OTHER.getVendorTypeNumber()).append(" end, ");

    sql.append("COLUMN_SIZE = case field.FieldType when 'string' then 32767 else 8 end, "); // TODO:based on type
    sql.append("ORDINAL_POSITION = fieldpos, ");
    sql.append("NULLABLE = case when field.IsNullable or field.IsMissable then 1 else 0 end ");
    sql.append("where array_length(dvname) between 1 and 2 ");

    sql.append("and array_length(dt.Derived.Record.Fields) > 0 ");
    if (catalog != null) {
      sql.append("and TABLE_CAT = \"").append(catalog).append("\" ");
    }
    if (schemaPattern != null) {
      sql.append("and if_null(TABLE_SCHEM, '') like \"").append(schemaPattern).append("\" ");
    }
    if (tableNamePattern != null) {
      sql.append("and TABLE_NAME like \"").append(tableNamePattern).append("\" ");
    }
    if (columnNamePattern != null) {
      sql.append("and COLUMN_NAME like \"").append(columnNamePattern).append("\" ");
    }
    sql.append("order by TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION");

    return sql.toString();
  }

  static String primaryKeysQuery(String catalog, String schema, String table) throws SQLException {
    StringBuilder sql = new StringBuilder(1024);
    sql.append("select TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, KEY_SEQ, null PK_NAME ");
    sql.append("from Metadata.`Dataset` ds unnest ds.InternalDetails.PrimaryKey pki at pkipos ");
    sql.append("let dvname = decode_dataverse_name(ds.DataverseName), ");
    sql.append("TABLE_CAT = dvname[0], ");
    sql.append("TABLE_SCHEM = case array_length(dvname) when 1 then null else dvname[1] end, ");
    sql.append("TABLE_NAME = ds.DatasetName, ");
    sql.append("COLUMN_NAME = pki[0], ");
    sql.append("KEY_SEQ = pkipos ");
    sql.append("where array_length(dvname) between 1 and 2 ");
    sql.append("and (every pk in ds.InternalDetails.PrimaryKey satisfies array_length(pk) = 1 end) ");
    sql.append("and (every si in ds.InternalDetails.KeySourceIndicator satisfies si = 0 end ) ");
    if (catalog != null) {
      sql.append("and TABLE_CAT = \"").append(catalog).append("\" ");
    }
    if (schema != null) {
      sql.append("and if_null(TABLE_SCHEM, '') like \"").append(schema).append("\" ");
    }
    if (table != null) {
      sql.append("and TABLE_NAME like \"").append(table).append("\" ");
    }
    sql.append("order by COLUMN_NAME");

    return sql.toString();
  }

  static List<LinkedHashMap<String, Object>> tableTypes() {
    return Stream.of(
      AnalyticsMetaDataQueries.getDatasetTerm(true),
      AnalyticsMetaDataQueries.getDatasetTerm(false),
      AnalyticsMetaDataQueries.getViewTerm(true),
      AnalyticsMetaDataQueries.getViewTerm(false)
    ).map(v -> {
      LinkedHashMap<String, Object> m = new LinkedHashMap<>();
      m.put("TABLE_TYPE", v);
      return m;
    }).collect(Collectors.toList());
  }

  static String getDatasetTerm(boolean tabular) {
    return tabular ? TABLE : SCHEMALESS + " " + TABLE;
  }

  static String getViewTerm(boolean tabular) {
    return tabular ? VIEW : SCHEMALESS + " " + VIEW;
  }

  static List<LinkedHashMap<String, Object>> typeInfo() {
    List<LinkedHashMap<String, Object>> types = new ArrayList<>();

    types.add(buildTypeInfo(AnalyticsDataType.BOOLEAN, 1, null, null, null, null, null, null));
    types.add(buildTypeInfo(AnalyticsDataType.TINYINT, 3, 10, 0, 0, false, null, null));

    types.add(buildTypeInfo(AnalyticsDataType.SMALLINT, 5, 10, 0, 0, false, null, null));
    types.add(buildTypeInfo(AnalyticsDataType.INTEGER, 10, 10, 0, 0, false, null, null));
    types.add(buildTypeInfo(AnalyticsDataType.BIGINT, 19, 10, 0, 0, false, null, null));
    types.add(buildTypeInfo(AnalyticsDataType.FLOAT, 7, 2, 0, 0, false, null, null));
    types.add(buildTypeInfo(AnalyticsDataType.DOUBLE, 15, 2, 0, 0, false, null, null));
    types.add(buildTypeInfo(AnalyticsDataType.DATE, 32, null, 0, 0, false, null, null));
    types.add(buildTypeInfo(AnalyticsDataType.TIME, 32, null, 0, 0, false, null, null));
    types.add(buildTypeInfo(AnalyticsDataType.DATETIME, 32, null, 0, 0, false, null, null));
    types.add(buildTypeInfo(AnalyticsDataType.YEARMONTHDURATION, 32, null, 0, 0, false, null, null));
    types.add(buildTypeInfo(AnalyticsDataType.DAYTIMEDURATION, 32, null, 0, 0, false, null, null));
    types.add(buildTypeInfo(AnalyticsDataType.DURATION, 32, null, 0, 0, false, null, null));
    types.add(buildTypeInfo(AnalyticsDataType.STRING, 32767, null, null, null, true, "'", "'"));
    types.add(buildTypeInfo(AnalyticsDataType.ARRAY, 32767, null, 0, 0, false, null, null));
    types.add(buildTypeInfo(AnalyticsDataType.OBJECT, 32767, null, 0, 0, false, null, null));

    return types;
  }

  static LinkedHashMap<String, Object> buildTypeInfo(AnalyticsDataType type, int precision, Integer precisionRadix,
                                                     Integer minScale, Integer maxScale, Boolean searchable,
                                                     String literalPrefix, String literalSuffix) {
    LinkedHashMap<String, Object> map = new LinkedHashMap<>();
    map.put("TYPE_NAME", type.getTypeName());
    map.put("DATA_TYPE", type.getJdbcType().getVendorTypeNumber());
    map.put("PRECISION", precision);
    map.put("LITERAL_PREFIX", literalPrefix);
    map.put("LITERAL_SUFFIX", literalSuffix);
    map.put("CREATE_PARAMS", null);
    map.put("NULLABLE", (short) DatabaseMetaData.typeNullable);
    map.put("CASE_SENSITIVE", false);
    map.put("SEARCHABLE", (short) (searchable == null ? DatabaseMetaData.typePredNone
      : searchable ? DatabaseMetaData.typeSearchable : DatabaseMetaData.typePredBasic));
    map.put("UNSIGNED_ATTRIBUTE", false);
    map.put("FIXED_PREC_SCALE", false);
    map.put("AUTO_INCREMENT", null);
    map.put("LOCAL_TYPE_NAME", null);
    map.put("MINIMUM_SCALE", minScale != null ? minScale.shortValue() : null);
    map.put("MAXIMUM_SCALE", maxScale != null ? maxScale.shortValue() : null);
    map.put("SQL_DATA_TYPE", type.getTypeTag());
    map.put("SQL_DATETIME_SUB", null);
    map.put("NUM_PREC_RADIX", precisionRadix != null ? precisionRadix : 10);

    return map;
  }

}