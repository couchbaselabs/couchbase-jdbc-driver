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

import com.couchbase.client.jdbc.CouchbaseDriver;
import com.couchbase.client.jdbc.sdk.ConnectionHandle;
import com.couchbase.client.jdbc.sdk.ConnectionManager;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class AnalyticsDatabaseMetaData implements DatabaseMetaData {

  private static final int METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8 = 251;

  private final AnalyticsConnection connection;

  AnalyticsDatabaseMetaData(AnalyticsConnection connection) {
    this.connection = connection;
  }

  @Override
  public AnalyticsConnection getConnection() throws SQLException {
    return connection;
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    String versionString = getDatabaseProductVersion();
    if (versionString == null || versionString.isEmpty()) {
      return 0;
    }
    return Integer.parseInt(versionString.split("\\.")[0]);
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    String versionString = getDatabaseProductVersion();
    if (versionString == null || versionString.isEmpty()) {
      return 0;
    }
    return Integer.parseInt(versionString.split("\\.")[1]);
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    ConnectionHandle handle = ConnectionManager.INSTANCE.handle(connection.connectionCoordinate());
    return handle.clusterVersion();
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException {
    return CouchbaseDriver.JDBC_MAJOR_VERSION;
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    return CouchbaseDriver.JDBC_MINOR_VERSION;
  }

  @Override
  public int getSQLStateType() throws SQLException {
    return sqlStateSQL;
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability) throws SQLException {
    return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
    return connection.createStatement().empty();
  }

  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
    return connection.createStatement().empty();
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
    return connection.createStatement().empty();
  }

  @Override
  public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
    return connection.createStatement().empty();
  }

  @Override
  public String getURL() throws SQLException {
    return connection.connectionCoordinate().url();
  }

  @Override
  public String getUserName() throws SQLException {
    return connection.connectionCoordinate().username();
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    return CouchbaseDriver.PRODUCT_NAME;
  }

  @Override
  public String getDriverName() throws SQLException {
    return CouchbaseDriver.DRIVER_NAME;
  }

  @Override
  public String getDriverVersion() throws SQLException {
    return CouchbaseDriver.DRIVER_VERSION.get();
  }

  @Override
  public int getDriverMajorVersion() {
    return CouchbaseDriver.DRIVER_MAJOR_VERSION.get();
  }

  @Override
  public int getDriverMinorVersion() {
    return CouchbaseDriver.DRIVER_MINOR_VERSION.get();
  }

  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
    return connection.createStatement().empty();
  }

  @Override
  public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
    return connection.createStatement().empty();
  }

  @Override
  public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
    return connection.createStatement().executeQuery(AnalyticsMetaDataQueries.tablesQuery(catalog, schemaPattern, tableNamePattern, types));
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    return connection.createStatement().executeQuery(
      AnalyticsMetaDataQueries.schemasQuery(
        connection.catalogDataverseMode(),
        connection.getCatalog(),
        null)
    );
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    return connection.createStatement().executeQuery(
      AnalyticsMetaDataQueries.schemasQuery(connection.catalogDataverseMode(), catalog, schemaPattern)
    );
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    return connection.createStatement().executeQuery(
      AnalyticsMetaDataQueries.catalogsQuery(connection.catalogDataverseMode())
    );
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    List<AnalyticsColumn> columns = Collections.singletonList(
      new AnalyticsColumn("TABLE_TYPE", AnalyticsDataType.STRING, false)
    );

    return connection.createStatement().fromData(AnalyticsMetaDataQueries.tableTypes(
      connection.catalogIncludesSchemaless()),
      columns
    );
  }

  @Override
  public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
    return connection.createStatement().executeQuery(
      AnalyticsMetaDataQueries.columnsQuery(catalog, schemaPattern, tableNamePattern, columnNamePattern)
    );
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
    return connection.createStatement().executeQuery(AnalyticsMetaDataQueries.primaryKeysQuery(catalog, schema, table));
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    List<AnalyticsColumn> columns = Arrays.asList(
      new AnalyticsColumn("TYPE_NAME", AnalyticsDataType.STRING, false),
      new AnalyticsColumn("DATA_TYPE", AnalyticsDataType.INTEGER, false),
      new AnalyticsColumn("PRECISION", AnalyticsDataType.INTEGER, true),
      new AnalyticsColumn("LITERAL_PREFIX", AnalyticsDataType.STRING, true),
      new AnalyticsColumn("LITERAL_SUFFIX", AnalyticsDataType.STRING, true),
      new AnalyticsColumn("CREATE_PARAMS", AnalyticsDataType.STRING, true),
      new AnalyticsColumn("NULLABLE", AnalyticsDataType.SMALLINT, true),
      new AnalyticsColumn("CASE_SENSITIVE", AnalyticsDataType.BOOLEAN, true),
      new AnalyticsColumn("SEARCHABLE", AnalyticsDataType.SMALLINT, true),
      new AnalyticsColumn("UNSIGNED_ATTRIBUTE", AnalyticsDataType.BOOLEAN, true),
      new AnalyticsColumn("FIXED_PREC_SCALE", AnalyticsDataType.BOOLEAN, true),
      new AnalyticsColumn("AUTO_INCREMENT", AnalyticsDataType.BOOLEAN, true),
      new AnalyticsColumn("LOCAL_TYPE_NAME", AnalyticsDataType.STRING, true),
      new AnalyticsColumn("MINIMUM_SCALE", AnalyticsDataType.SMALLINT, true),
      new AnalyticsColumn("MAXIMUM_SCALE", AnalyticsDataType.SMALLINT, true),
      new AnalyticsColumn("SQL_DATA_TYPE", AnalyticsDataType.INTEGER, true),
      new AnalyticsColumn("SQL_DATETIME_SUB", AnalyticsDataType.INTEGER, true),
      new AnalyticsColumn("NUM_PREC_RADIX", AnalyticsDataType.INTEGER, true)
    );

    return connection.createStatement().fromData(AnalyticsMetaDataQueries.typeInfo(), columns);
  }

  @Override
  public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
    return connection.createStatement().empty();
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
    return connection.createStatement().empty();
  }

  @Override
  public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
    return connection.createStatement().empty();
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
    return connection.createStatement().empty();
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
    return connection.createStatement().empty();
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
    return connection.createStatement().empty();
  }

  @Override
  public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
    return connection.createStatement().empty();
  }

  @Override
  public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
    return connection.createStatement().empty();
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    return connection.createStatement().empty();
  }

  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
    return connection.createStatement().empty();
  }

  @Override
  public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
    return connection.createStatement().empty();
  }

  @Override
  public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
    return connection.createStatement().empty();
  }

  @Override
  public boolean allProceduresAreCallable() throws SQLException {
    return false;
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException {
    return true;
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return false;
  }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    return false;
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    return true;
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    return false;
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    return false;
  }

  @Override
  public boolean usesLocalFiles() throws SQLException {
    return false;
  }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    return true;
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    return true;
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    return "`";
  }

  @Override
  public String getSQLKeywords() throws SQLException {
    // keywords that are not also SQL:2003 keywords
    return "adapter,apply,asc,autogenerated,btree,closed,compaction,compact,correlate,collection,dataset,"
      + "dataverse,definition,desc,disconnect,div,explain,enforced,every,feed,flatten,fulltext,hints,if,"
      + "index,ingestion,internal,keyword,key,known,letting,let,limit,load,missing,mod,nodegroup,ngram,"
      + "offset,path,policy,pre-sorted,raw,refresh,returning,rtree,run,satisfies,secondary,some,stop,"
      + "synonym,temporary,type,upsert,use,view,write";
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    // NOTE: JDBC escape clause is not yet supported
    // "add,div,mod,mult,neg,sub,abs,acos,asin,atan,atan2,ceil,cos,deg,degrees,e,exp,ln,log,floor,inf,nan,neginf,pi,posinf,power,rad,radians,random,round,sign,sin,sqrt,tan,trunc";
    return "";
  }

  @Override
  public String getStringFunctions() throws SQLException {
    // NOTE: JDBC escape clause is not yet supported
    // "contains,initcap,length,lower,ltrim,position,pos,regex_contains,regex_like,regex_position,regex_pos,regex_replace,repeat,replace,rtrim,split,substr,title,trim,upper";
    return "";
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    // NOTE: JDBC escape clause is not yet supported
    return "";
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    // TODO:review
    return "current_date,current_time,current_datetime";
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    return "\\";
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    return "";
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    return true;
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    return false;
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    return true;
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    return "schema";
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    return "procedure";
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    return "catalog";
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    return true;
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    return ".";
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    return true;
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxConnections() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    return METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8;
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    return METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8;
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    return METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8;
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    return 0;
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    return true;
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxStatements() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    return METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8;
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    return 0;
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_READ_COMMITTED;
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
    return Connection.TRANSACTION_READ_COMMITTED == level;
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    return true;
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    return false;
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException {
    return ResultSet.TYPE_SCROLL_SENSITIVE == type;
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
    return ResultSet.TYPE_SCROLL_SENSITIVE == type && concurrency == ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean ownDeletesAreVisible(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean ownInsertsAreVisible(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean othersDeletesAreVisible(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean othersInsertsAreVisible(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean updatesAreDetected(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean deletesAreDetected(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean insertsAreDetected(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    return false;
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException {
    return false;
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    return RowIdLifetime.ROWID_UNSUPPORTED;
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    return false;
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    return false;
  }

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }
}
