// src/include/lance_geo_scan.hpp
// Bind data and registration for the lance_rtree_index_scan table function
// and the LanceRTreeScanOptimizer (OptimizerExtension).
#pragma once

#include "duckdb.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "lance_geo_types.hpp"

namespace duckdb {

/// Bind data for the lance_rtree_index_scan table function.
/// Holds the dataset URI, geometry column name, query bbox, projection,
/// and schema information for Arrow -> DuckDB conversion.
struct LanceGeoScanBindData : public TableFunctionData {
  /// Lance dataset URI (passed to lance_open_dataset at execution time).
  string dataset_uri;

  /// Name of the geometry column that has an RTree index.
  string geometry_column;

  /// Query bounding box (f64).
  LanceBBox bbox;

  /// Projected columns (empty = all columns).
  vector<string> columns;

  /// Arrow schema and type information for Arrow -> DuckDB conversion.
  ArrowSchemaWrapper schema_root;
  ArrowTableSchema arrow_table;
  vector<string> names;
  vector<LogicalType> types;

  LanceGeoScanBindData() = default;
};

/// Register the lance_rtree_index_scan table function and the
/// LanceRTreeScanOptimizer into the DuckDB instance.
void RegisterLanceGeoScan(ExtensionLoader &loader, DBConfig &config);

} // namespace duckdb
