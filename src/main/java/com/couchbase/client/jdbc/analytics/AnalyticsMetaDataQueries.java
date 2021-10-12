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

public class AnalyticsMetaDataQueries {

  private static final String PK_NAME_SUFFIX = "_pk";
  private static final String FK_NAME_SUFFIX = "_fk";

  private static final String SCHEMALESS = "SCHEMALESS";
  private static final String TABLE = "TABLE";
  private static final String VIEW = "VIEW";

  private AnalyticsMetaDataQueries() {}

  static String catalogsQuery(AnalyticsCatalogDataverseMode mode) {
    StringBuilder sql = new StringBuilder();

    sql.append("select TABLE_CAT ");
    sql.append("from Metadata.`Dataverse` ");
    switch (mode) {
      case CATALOG:
        sql.append("let TABLE_CAT = DataverseName ");
        break;
      case CATALOG_SCHEMA:
        sql.append("let name = decode_dataverse_name(DataverseName), ");
        sql.append("TABLE_CAT = name[0] ");
        sql.append("where (array_length(name) between 1 and 2) ");
        sql.append("group by TABLE_CAT ");
        break;
      default:
        throw new IllegalStateException();
    }

    sql.append("order by TABLE_CAT");

    return sql.toString();
  }

  static String schemasQuery(AnalyticsCatalogDataverseMode mode, String catalog, String schemaPattern) {
    StringBuilder sql = new StringBuilder();

    sql.append("select TABLE_SCHEM, TABLE_CATALOG ");
    sql.append("from Metadata.`Dataverse` ");
    sql.append("let ");
    switch (mode) {
      case CATALOG:
        sql.append("TABLE_CATALOG = DataverseName, ");
        sql.append("TABLE_SCHEM = null ");
        sql.append("where true ");
        break;
      case CATALOG_SCHEMA:
        sql.append("name = decode_dataverse_name(DataverseName), ");
        sql.append("TABLE_CATALOG = name[0], ");
        sql.append("TABLE_SCHEM = case array_length(name) when 1 then null else name[1] end ");
        sql.append("where (array_length(name) between 1 and 2) ");
        break;
      default:
        throw new IllegalStateException();
    }
    if (catalog != null) {
      sql.append("and (TABLE_CATALOG = \"").append(catalog).append("\") ");
    }
    if (schemaPattern != null) {
      sql.append("and (if_null(TABLE_SCHEM, '') like \"").append(schemaPattern).append("\") ");
    }
    sql.append("order by TABLE_CATALOG, TABLE_SCHEM");

    return sql.toString();
  }

  static String tablesQuery(AnalyticsCatalogDataverseMode mode, boolean catalogIncludesSchemaless,
                            String catalog, String schemaPattern, String tableNamePattern, String[] types) {
    String datasetTermTabular = getDatasetTerm(true);
    String datasetTermNonTabular = getDatasetTerm(false);
    String viewTermTabular = getViewTerm(true);
    String viewTermNonTabular = getViewTerm(false);

    StringBuilder sql = new StringBuilder(1024);

    List<String> typesList = types != null ? Arrays.asList(types) : null;

    sql.append("select TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, null REMARKS, null TYPE_CAT, ");
    sql.append("null TYPE_SCHEM, null TYPE_NAME, null SELF_REFERENCING_COL_NAME, null REF_GENERATION ");
    sql.append("from Metadata.`Dataset` ds join Metadata.`Datatype` dt ");
    sql.append("on ds.DatatypeDataverseName = dt.DataverseName and ds.DatatypeName = dt.DatatypeName ");
    sql.append("let ");
    switch (mode) {
      case CATALOG:
        sql.append("TABLE_CAT = ds.DataverseName, ");
        sql.append("TABLE_SCHEM = null, ");
        break;
      case CATALOG_SCHEMA:
        sql.append("dvname = decode_dataverse_name(ds.DataverseName), ");
        sql.append("TABLE_CAT = dvname[0], ");
        sql.append("TABLE_SCHEM = case array_length(dvname) when 1 then null else dvname[1] end, ");
        break;
      default:
        throw new IllegalStateException();
    }
    sql.append("TABLE_NAME = ds.DatasetName, ");
    sql.append("isDataset = (ds.DatasetType = 'INTERNAL' or ds.DatasetType = 'EXTERNAL'), ");
    sql.append("isView = ds.DatasetType = 'VIEW', ");
    sql.append("hasFields = array_length(dt.Derived.Record.Fields) > 0, ");
    sql.append("TABLE_TYPE = case ");
    sql.append("when isDataset then (case when hasFields then '").append(datasetTermTabular).append("' else '")
      .append(datasetTermNonTabular).append("' end) ");
    sql.append("when isView then (case when hasFields then '").append(viewTermTabular).append("' else '")
      .append(viewTermNonTabular).append("' end) ");
    sql.append("else null end ");

    sql.append("where ");
    sql.append("(TABLE_TYPE ").append(types != null ? "in " + Mapper.encodeAsString(typesList) : "is not null").append(") ");
    if (catalog != null) {
      sql.append("and (TABLE_CAT = \"").append(catalog).append("\") ");
    }
    if (schemaPattern != null) {
      sql.append("and (if_null(TABLE_SCHEM, '') like \"").append(schemaPattern).append("\") ");
    }
    if (tableNamePattern != null) {
      sql.append("and (TABLE_NAME like \"").append(tableNamePattern).append("\") ");
    }
    switch (mode) {
      case CATALOG:
        break;
      case CATALOG_SCHEMA:
        sql.append("and (array_length(dvname) between 1 and 2) ");
        break;
      default:
        throw new IllegalStateException();
    }
    if (!catalogIncludesSchemaless) {
      sql.append("and hasFields ");
    }

    sql.append("order by TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME");

    return sql.toString();
  }

  static String columnsQuery(AnalyticsCatalogDataverseMode mode, String catalog, String schemaPattern,
                             String tableNamePattern, String columnNamePattern) {
    StringBuilder sql = new StringBuilder();

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
    sql.append("let ");
    switch (mode) {
      case CATALOG:
        sql.append("TABLE_CAT = ds.DataverseName, ");
        sql.append("TABLE_SCHEM = null, ");
        break;
      case CATALOG_SCHEMA:
        sql.append("dvname = decode_dataverse_name(ds.DataverseName), ");
        sql.append("TABLE_CAT = dvname[0], ");
        sql.append("TABLE_SCHEM = case array_length(dvname) when 1 then null else dvname[1] end, ");
        break;
      default:
        throw new IllegalStateException();
    }
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

    sql.append("where (array_length(dt.Derived.Record.Fields) > 0) ");
    if (catalog != null) {
      sql.append("and (TABLE_CAT = \"").append(catalog).append("\") ");
    }
    if (schemaPattern != null) {
      sql.append("and (if_null(TABLE_SCHEM, '') like \"").append(schemaPattern).append("\") ");
    }
    if (tableNamePattern != null) {
      sql.append("and (TABLE_NAME like \"").append(tableNamePattern).append("\") ");
    }
    if (columnNamePattern != null) {
      sql.append("and (COLUMN_NAME like \"").append(columnNamePattern).append("\") ");
    }
    switch (mode) {
      case CATALOG:
        break;
      case CATALOG_SCHEMA:
        sql.append("and (array_length(dvname) between 1 and 2) ");
        break;
      default:
        throw new IllegalStateException();
    }

    sql.append("order by TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION");

    return sql.toString();
  }

  static String primaryKeysQuery(AnalyticsCatalogDataverseMode mode, boolean catalogIncludesSchemaless,
                                 String catalog, String schema, String table) throws SQLException {
    StringBuilder sql = new StringBuilder(1024);

    sql.append("select TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, KEY_SEQ, PK_NAME ");
    sql.append("from Metadata.`Dataset` ds ");
    sql.append("join Metadata.`Datatype` dt ");
    sql.append("on ds.DatatypeDataverseName = dt.DataverseName and ds.DatatypeName = dt.DatatypeName ");
    sql.append("unnest coalesce(ds.InternalDetails, ds.ExternalDetails, ds.ViewDetails).PrimaryKey pki at pkipos ");
    sql.append("let ");
    sql.append("hasFields = array_length(dt.Derived.Record.Fields) > 0, ");
    switch (mode) {
      case CATALOG:
        sql.append("TABLE_CAT = ds.DataverseName, ");
        sql.append("TABLE_SCHEM = null, ");
        break;
      case CATALOG_SCHEMA:
        sql.append("dvname = decode_dataverse_name(ds.DataverseName), ");
        sql.append("TABLE_CAT = dvname[0], ");
        sql.append("TABLE_SCHEM = case array_length(dvname) when 1 then null else dvname[1] end, ");
        break;
      default:
        throw new IllegalStateException();
    }
    sql.append("TABLE_NAME = ds.DatasetName, ");
    sql.append("COLUMN_NAME = pki[0], ");
    sql.append("KEY_SEQ = pkipos, ");
    sql.append("PK_NAME = TABLE_NAME || '").append(PK_NAME_SUFFIX).append("', ");
    sql.append("dsDetails = coalesce(ds.InternalDetails, ds.ExternalDetails, ds.ViewDetails) ");
    sql.append("where (every pk in dsDetails.PrimaryKey satisfies array_length(pk) = 1 end) ");
    sql.append("and (every si in dsDetails.KeySourceIndicator satisfies si = 0 end ) ");
    if (catalog != null) {
      sql.append("and (TABLE_CAT = \"").append(catalog).append("\") ");
    }
    if (schema != null) {
      sql.append("and (if_null(TABLE_SCHEM, '') like \"").append(schema).append("\") ");
    }
    if (table != null) {
      sql.append("and (TABLE_NAME like \"").append(table).append("\") ");
    }
    switch (mode) {
      case CATALOG:
        break;
      case CATALOG_SCHEMA:
        sql.append("and (array_length(dvname) between 1 and 2) ");
        break;
      default:
        throw new IllegalStateException();
    }
    if (!catalogIncludesSchemaless) {
      sql.append("and hasFields ");
    }

    sql.append("order by COLUMN_NAME");

    return sql.toString();
  }

  static List<LinkedHashMap<String, Object>> tableTypes(boolean includeSchemaless) {
    List<String> types = new ArrayList<>();
    types.add(AnalyticsMetaDataQueries.getDatasetTerm(true));
    types.add(AnalyticsMetaDataQueries.getViewTerm(true));

    if (includeSchemaless) {
      types.add(AnalyticsMetaDataQueries.getDatasetTerm(false));
      types.add(AnalyticsMetaDataQueries.getViewTerm(false));
    }

    return types.stream().map(v -> {
      LinkedHashMap<String, Object> m = new LinkedHashMap<>();
      m.put("TABLE_TYPE", v);
      return m;
    }).collect(Collectors.toList());
  }

  static String importedKeysQuery(AnalyticsCatalogDataverseMode mode, boolean catalogIncludesSchemaless,
                                  String catalog, String schema, String table) {
    return importedExportedKeysQuery(mode, catalogIncludesSchemaless, null, null, null, catalog, schema, table,
      false);
  }

  static String exportedKeysQuery(AnalyticsCatalogDataverseMode mode, boolean catalogIncludesSchemaless,
                                  String catalog, String schema, String table) {
    return importedExportedKeysQuery(mode, catalogIncludesSchemaless, catalog, schema, table, null, null, null,
      true);
  }

  static String crossReferenceQuery(AnalyticsCatalogDataverseMode mode, boolean catalogIncludesSchemaless,
                                    String parentCatalog, String parentSchema, String parentTable,
                                           String foreignCatalog, String foreignSchema, String foreignTable) {
    return importedExportedKeysQuery(mode, catalogIncludesSchemaless, parentCatalog, parentSchema, parentTable,
      foreignCatalog, foreignSchema, foreignTable, true);
  }

  private static String importedExportedKeysQuery(AnalyticsCatalogDataverseMode mode, boolean catalogIncludesSchemaless,
                                                  String pkCatalog, String pkSchema,
                                                  String pkTable, String fkCatalog, String fkSchema,
                                                  String fkTable, boolean orderByFk) {
    StringBuilder sql = new StringBuilder(2048);

    sql.append("select PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, PKCOLUMN_NAME, ");
    sql.append("FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, FKCOLUMN_NAME, KEY_SEQ, ");
    sql.append(DatabaseMetaData.importedKeyNoAction).append(" UPDATE_RULE, ");
    sql.append(DatabaseMetaData.importedKeyNoAction).append(" DELETE_RULE, ");
    sql.append("FK_NAME, PK_NAME, ");
    sql.append(DatabaseMetaData.importedKeyInitiallyDeferred).append(" DEFERRABILITY ");
    sql.append("from Metadata.`Dataset` ds ");
    sql.append("join Metadata.`Datatype` dt ");
    sql.append("on ds.DatatypeDataverseName = dt.DataverseName and ds.DatatypeName = dt.DatatypeName ");
    sql.append("unnest coalesce(ds.InternalDetails, ds.ExternalDetails, ds.ViewDetails).ForeignKeys fk at fkpos ");
    sql.append("join Metadata.`Dataset` ds2 ");
    sql.append("on fk.RefDataverseName = ds2.DataverseName and fk.RefDatasetName = ds2.DatasetName ");
    sql.append("unnest fk.ForeignKey fki at fkipos ");
    sql.append("let ");
    sql.append("hasFields = array_length(dt.Derived.Record.Fields) > 0, ");
    switch (mode) {
      case CATALOG:
        sql.append("FKTABLE_CAT = ds.DataverseName, ");
        sql.append("FKTABLE_SCHEM = null, ");
        sql.append("PKTABLE_CAT = ds2.DataverseName, ");
        sql.append("PKTABLE_SCHEM = null, ");
        break;
      case CATALOG_SCHEMA:
        sql.append("dvname = decode_dataverse_name(ds.DataverseName), ");
        sql.append("FKTABLE_CAT = dvname[0], ");
        sql.append("FKTABLE_SCHEM = case array_length(dvname) when 1 then null else dvname[1] end, ");
        sql.append("dvname2 = decode_dataverse_name(ds2.DataverseName), ");
        sql.append("PKTABLE_CAT = dvname2[0], ");
        sql.append("PKTABLE_SCHEM = case array_length(dvname2) when 1 then null else dvname2[1] end, ");
        break;
      default:
        throw new IllegalStateException();
    }
    sql.append("ds2Details = coalesce(ds2.InternalDetails, ds2.ExternalDetails, ds2.ViewDetails), ");
    sql.append("FKTABLE_NAME = ds.DatasetName, ");
    sql.append("PKTABLE_NAME = ds2.DatasetName, ");
    sql.append("FKCOLUMN_NAME = fki[0], ");
    sql.append("PKCOLUMN_NAME = ds2Details.PrimaryKey[fkipos-1][0], ");
    sql.append("KEY_SEQ = fkipos, ");
    sql.append("PK_NAME = PKTABLE_NAME || '").append(PK_NAME_SUFFIX).append("', ");
    sql.append("FK_NAME = FKTABLE_NAME || '").append(FK_NAME_SUFFIX).append("_' || string(fkpos) ");
    sql.append("where (every fki2 in fk.ForeignKey satisfies array_length(fki2) = 1 end) ");
    sql.append("and (every fksi in fk.KeySourceIndicator satisfies fksi = 0 end ) ");
    sql.append("and (every pki in ds2Details.PrimaryKey satisfies array_length(pki) = 1 end) ");
    sql.append("and (every pksi in ds2Details.KeySourceIndicator satisfies pksi = 0 end) ");

    if (pkCatalog != null) {
      sql.append("and (").append("PKTABLE_CAT").append(" = \"").append(pkCatalog).append("\") ");
    }
    if (pkSchema != null) {
      sql.append("and (if_null(").append("PKTABLE_SCHEM").append(", '') like \"").append(pkSchema).append("\") ");
    }
    if (pkTable != null) {
      sql.append("and (").append("PKTABLE_NAME").append(" like \"").append(pkTable).append("\") ");
    }

    if (fkCatalog != null) {
      sql.append("and (").append("FKTABLE_CAT").append(" = \"").append(fkCatalog).append("\") ");
    }
    if (fkSchema != null) {
      sql.append("and (if_null(").append("FKTABLE_SCHEM").append(", '') like \"").append(fkSchema).append("\") ");
    }
    if (fkTable != null) {
      sql.append("and (").append("FKTABLE_NAME").append(" like \"").append(fkTable).append("\") ");
    }

    switch (mode) {
      case CATALOG:
        break;
      case CATALOG_SCHEMA:
        sql.append("and (array_length(dvname) between 1 and 2) ");
        sql.append("and (array_length(dvname2) between 1 and 2) ");
        break;
      default:
        throw new IllegalStateException();
    }
    if (!catalogIncludesSchemaless) {
      sql.append("and hasFields ");
    }

    sql.append("order by ").append(
        orderByFk ? "FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME" : "PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME")
      .append(", KEY_SEQ");

    return sql.toString();
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
