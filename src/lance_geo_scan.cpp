// src/lance_geo_scan.cpp
// Implements:
//   1. lance_rtree_index_scan - a TableFunction that calls Lance RTree FFI
//   2. LanceRTreeScanOptimizer - an OptimizerExtension that rewrites:
//        Filter(ST_Intersects(geom_col, ST_MakeEnvelope(xmin,ymin,xmax,ymax)))
//          └─ LogicalGet(lance_scan / __lance_table_scan / __lance_namespace_scan)
//      Into:
//        LogicalGet(lance_rtree_index_scan, LanceGeoScanBindData)

#include "lance_geo_scan.hpp"
#include "lance_common.hpp"
#include "lance_ffi.hpp"
#include "lance_table_entry.hpp"

#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include <atomic>
#include <cstring>

namespace duckdb {

// ─── Forward declarations ────────────────────────────────────────────────────

static TableFunction GetLanceGeoScanFunction();
static void PopulateGeoScanSchema(ClientContext &context, void *dataset,
                                  LanceGeoScanBindData &bind_data);

// ─── Global State ────────────────────────────────────────────────────────────

struct LanceGeoScanGlobalState : public GlobalTableFunctionState {
  void *dataset = nullptr;
  void *stream = nullptr;
  bool stream_exhausted = false;
  std::atomic<idx_t> lines_read{0};

  ~LanceGeoScanGlobalState() override {
    if (stream) {
      lance_close_stream(stream);
    }
    if (dataset) {
      lance_close_dataset(dataset);
    }
  }

  idx_t MaxThreads() const override { return 1; }
};

struct LanceGeoScanLocalState : public ArrowScanLocalState {
  explicit LanceGeoScanLocalState(unique_ptr<ArrowArrayWrapper> chunk,
                                  ClientContext &ctx)
      : ArrowScanLocalState(std::move(chunk), ctx) {}
};

// ─── TableFunction: lance_rtree_index_scan ──────────────────────────────────

static unique_ptr<GlobalTableFunctionState>
LanceGeoScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
  auto &bind_data = input.bind_data->Cast<LanceGeoScanBindData>();
  auto state = make_uniq<LanceGeoScanGlobalState>();

  // Open the Lance dataset
  state->dataset = LanceOpenDataset(context, bind_data.dataset_uri);
  if (!state->dataset) {
    throw IOException("lance_rtree_index_scan: failed to open dataset: " +
                      bind_data.dataset_uri + LanceFormatErrorSuffix());
  }

  // Build column projection list
  vector<const char *> col_ptrs;
  for (auto &c : bind_data.columns) {
    col_ptrs.push_back(c.c_str());
  }

  // Call Lance FFI: RTree search + streaming result
  state->stream = lance_create_rtree_scan_stream(
      state->dataset, bind_data.geometry_column.c_str(), bind_data.bbox.min_x,
      bind_data.bbox.min_y, bind_data.bbox.max_x, bind_data.bbox.max_y,
      col_ptrs.empty() ? nullptr : col_ptrs.data(), col_ptrs.size());

  if (!state->stream) {
    throw IOException(
        "lance_rtree_index_scan: lance_create_rtree_scan_stream failed: " +
        LanceConsumeLastError());
  }

  return std::move(state);
}

static unique_ptr<LocalTableFunctionState>
LanceGeoScanInitLocal(ExecutionContext &context, TableFunctionInitInput &,
                      GlobalTableFunctionState *) {
  auto chunk = make_uniq<ArrowArrayWrapper>();
  return make_uniq<LanceGeoScanLocalState>(std::move(chunk), context.client);
}

static void LanceGeoScanExecute(ClientContext &context,
                                TableFunctionInput &data_p,
                                DataChunk &output) {
  auto &bind_data = data_p.bind_data->Cast<LanceGeoScanBindData>();
  auto &global_state = data_p.global_state->Cast<LanceGeoScanGlobalState>();
  auto &local_state = data_p.local_state->Cast<LanceGeoScanLocalState>();

  if (global_state.stream_exhausted) {
    output.SetCardinality(0);
    return;
  }

  while (true) {
    if (local_state.chunk_offset >=
        NumericCast<idx_t>(local_state.chunk->arrow_array.length)) {
      // Need next batch from the stream
      void *batch = nullptr;
      auto rc = lance_stream_next(global_state.stream, &batch);
      if (rc == 1) {
        // Stream exhausted
        lance_close_stream(global_state.stream);
        global_state.stream = nullptr;
        global_state.stream_exhausted = true;
        output.SetCardinality(0);
        return;
      }
      if (rc != 0) {
        throw IOException(
            "lance_rtree_index_scan: Failed to read next RecordBatch: " +
            LanceConsumeLastError());
      }

      auto new_chunk = make_shared_ptr<ArrowArrayWrapper>();
      memset(&new_chunk->arrow_array, 0, sizeof(new_chunk->arrow_array));
      ArrowSchema tmp_schema;
      memset(&tmp_schema, 0, sizeof(tmp_schema));

      if (lance_batch_to_arrow(batch, &new_chunk->arrow_array, &tmp_schema) !=
          0) {
        lance_free_batch(batch);
        throw IOException("lance_rtree_index_scan: Failed to export Arrow: " +
                          LanceConsumeLastError());
      }
      lance_free_batch(batch);

      if (tmp_schema.release) {
        tmp_schema.release(&tmp_schema);
      }

      local_state.chunk = std::move(new_chunk);
      local_state.Reset();
    }

    auto remaining = NumericCast<idx_t>(local_state.chunk->arrow_array.length) -
                     local_state.chunk_offset;
    auto output_size = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining);
    auto start = global_state.lines_read.fetch_add(output_size);

    output.SetCardinality(output_size);
    ArrowTableFunction::ArrowToDuckDB(
        local_state, bind_data.arrow_table.GetColumns(), output, start, false);
    local_state.chunk_offset += output_size;

    if (output.size() == 0) {
      continue;
    }
    output.Verify();
    return;
  }
}

static unique_ptr<FunctionData>
LanceGeoScanBind(ClientContext &context, TableFunctionBindInput &input,
                 vector<LogicalType> &return_types, vector<string> &names) {
  // Direct invocation: lance_rtree_index_scan(uri, geom_col, xmin, ymin, xmax, ymax)
  if (input.inputs.size() < 6) {
    throw InvalidInputException(
        "lance_rtree_index_scan requires (uri, geometry_column, "
        "xmin, ymin, xmax, ymax)");
  }

  auto bind_data = make_uniq<LanceGeoScanBindData>();
  bind_data->dataset_uri = input.inputs[0].GetValue<string>();
  bind_data->geometry_column = input.inputs[1].GetValue<string>();
  bind_data->bbox = LanceBBox(
      input.inputs[2].DefaultCastAs(LogicalType::DOUBLE).GetValue<double>(),
      input.inputs[3].DefaultCastAs(LogicalType::DOUBLE).GetValue<double>(),
      input.inputs[4].DefaultCastAs(LogicalType::DOUBLE).GetValue<double>(),
      input.inputs[5].DefaultCastAs(LogicalType::DOUBLE).GetValue<double>());

  // Open dataset and populate schema
  void *dataset = LanceOpenDataset(context, bind_data->dataset_uri);
  if (!dataset) {
    throw IOException("lance_rtree_index_scan: failed to open dataset: " +
                      bind_data->dataset_uri + LanceFormatErrorSuffix());
  }

  // Populate schema
  try {
    PopulateGeoScanSchema(context, dataset, *bind_data);
  } catch (...) {
    lance_close_dataset(dataset);
    throw;
  }

  // All columns are projected by default when called directly
  bind_data->columns = bind_data->names;

  lance_close_dataset(dataset);

  return_types = bind_data->types;
  names = bind_data->names;
  return std::move(bind_data);
}

static TableFunction GetLanceGeoScanFunction() {
  TableFunction func(
      "lance_rtree_index_scan",
      {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::DOUBLE,
       LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE},
      LanceGeoScanExecute, LanceGeoScanBind, LanceGeoScanInitGlobal,
      LanceGeoScanInitLocal);
  func.projection_pushdown = false; // Projection handled at bind time
  return func;
}

// ─── Optimizer: LanceRTreeScanOptimizer ─────────────────────────────────────
// Rewrites:
//   LogicalFilter(ST_Intersects(geom_col, ST_MakeEnvelope(xmin,ymin,xmax,ymax)))
//     └─ LogicalGet(lance_scan / __lance_table_scan / __lance_namespace_scan)
// Into:
//   LogicalGet(lance_rtree_index_scan, LanceGeoScanBindData)

static bool IsLanceScanFunction(const string &name) {
  return name == "lance_scan" || name == "__lance_scan" ||
         name == "__lance_table_scan" || name == "__lance_namespace_scan";
}

/// Try to extract a bounding box from a ST_MakeEnvelope(xmin,ymin,xmax,ymax)
/// function call.
static bool TryExtractBBoxFromMakeEnvelope(const Expression &expr,
                                           LanceBBox &out) {
  if (expr.type != ExpressionType::BOUND_FUNCTION)
    return false;
  auto &func = expr.Cast<BoundFunctionExpression>();
  if (func.function.name != "ST_MakeEnvelope" &&
      func.function.name != "st_makeenvelope") {
    return false;
  }
  if (func.children.size() < 4)
    return false;

  auto get_double = [](const Expression &e, double &out) -> bool {
    if (e.type != ExpressionType::VALUE_CONSTANT)
      return false;
    auto &c = e.Cast<BoundConstantExpression>();
    if (c.value.IsNull())
      return false;
    try {
      out =
          c.value.DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
      return true;
    } catch (...) {
      return false;
    }
  };

  double xmin, ymin, xmax, ymax;
  if (!get_double(*func.children[0], xmin))
    return false;
  if (!get_double(*func.children[1], ymin))
    return false;
  if (!get_double(*func.children[2], xmax))
    return false;
  if (!get_double(*func.children[3], ymax))
    return false;

  out = LanceBBox(xmin, ymin, xmax, ymax);
  return true;
}

/// Try to extract (geometry_column_name, bbox) from a spatial predicate.
/// Handles: ST_Intersects(col, ST_MakeEnvelope(...)) and
///          ST_Intersects(ST_MakeEnvelope(...), col) etc.
static bool TryExtractSpatialPredicate(const Expression &expr,
                                       const LogicalGet &get,
                                       const TableCatalogEntry &table,
                                       string &out_geo_column,
                                       LanceBBox &out_bbox) {
  if (expr.type != ExpressionType::BOUND_FUNCTION)
    return false;
  auto &func = expr.Cast<BoundFunctionExpression>();
  if (!IsLanceSpatialPredicate(func.function.name.c_str()))
    return false;
  if (func.children.size() < 2)
    return false;

  // Try both argument orders: (col, bbox) and (bbox, col)
  for (int order = 0; order < 2; ++order) {
    auto &col_expr = *func.children[order];
    auto &bbox_expr = *func.children[1 - order];

    // Column reference
    if (col_expr.type != ExpressionType::BOUND_COLUMN_REF)
      continue;
    auto &colref = col_expr.Cast<BoundColumnRefExpression>();
    if (colref.binding.table_index != get.table_index)
      continue;

    auto &col_ids = get.GetColumnIds();
    if (colref.binding.column_index >= col_ids.size())
      continue;
    auto col_id = col_ids[colref.binding.column_index].GetPrimaryIndex();
    if (col_id >= table.GetColumns().LogicalColumnCount())
      continue;

    auto &col_name =
        table.GetColumns().GetColumn(LogicalIndex(col_id)).Name();

    // BBox from ST_MakeEnvelope
    LanceBBox bbox;
    if (!TryExtractBBoxFromMakeEnvelope(bbox_expr, bbox)) {
      continue;
    }

    out_geo_column = col_name;
    out_bbox = bbox;
    return true;
  }
  return false;
}

/// Check if the Lance dataset has an RTree index on the given column.
/// Uses lance_dataset_list_indices() which returns a JSON string.
static bool LanceHasRTreeIndex(void *dataset, const string &column) {
  auto *indices_json = lance_dataset_list_indices(dataset);
  if (!indices_json)
    return false;

  string json_str = indices_json;
  lance_free_string(indices_json);

  // Simple heuristic: look for "RTree" and the column name in the JSON.
  // A more robust implementation would parse the JSON properly.
  return json_str.find("RTree") != string::npos &&
         json_str.find(column) != string::npos;
}

/// Populate LanceGeoScanBindData with schema information from a dataset.
static void PopulateGeoScanSchema(ClientContext &context, void *dataset,
                                  LanceGeoScanBindData &bind_data) {
  auto *schema_handle = lance_get_schema(dataset);
  if (!schema_handle) {
    throw IOException("Failed to get schema from Lance dataset" +
                      LanceFormatErrorSuffix());
  }

  memset(&bind_data.schema_root.arrow_schema, 0,
         sizeof(bind_data.schema_root.arrow_schema));
  if (lance_schema_to_arrow(schema_handle,
                            &bind_data.schema_root.arrow_schema) != 0) {
    lance_free_schema(schema_handle);
    throw IOException("Failed to export schema to Arrow" +
                      LanceFormatErrorSuffix());
  }
  lance_free_schema(schema_handle);

  auto &config = DBConfig::GetConfig(context);
  ArrowTableFunction::PopulateArrowTableSchema(
      config, bind_data.arrow_table, bind_data.schema_root.arrow_schema);
  bind_data.names = bind_data.arrow_table.GetNames();
  bind_data.types = bind_data.arrow_table.GetTypes();
}

static void OptimizeRecursive(ClientContext &context,
                              unique_ptr<LogicalOperator> &node);

static bool TryOptimizeFilter(ClientContext &context,
                              unique_ptr<LogicalOperator> &node) {
  if (node->type != LogicalOperatorType::LOGICAL_FILTER)
    return false;
  auto &filter = node->Cast<LogicalFilter>();
  if (filter.children.size() != 1)
    return false;
  if (filter.children[0]->type != LogicalOperatorType::LOGICAL_GET)
    return false;

  auto &get = filter.children[0]->Cast<LogicalGet>();
  if (!IsLanceScanFunction(get.function.name))
    return false;

  // Get the LanceTableEntry
  auto table_opt = get.GetTable();
  if (!table_opt)
    return false;
  auto *lance_table = dynamic_cast<LanceTableEntry *>(&*table_opt);
  if (!lance_table)
    return false;

  // Try to extract a spatial predicate from each filter expression
  for (idx_t i = 0; i < filter.expressions.size(); i++) {
    string geo_column;
    LanceBBox bbox;
    if (!TryExtractSpatialPredicate(*filter.expressions[i], get, *lance_table,
                                    geo_column, bbox)) {
      continue;
    }

    // Verify RTree index exists on this column
    string display_uri;
    void *dataset = LanceOpenDatasetForTable(context, *lance_table, display_uri);
    if (!dataset)
      continue;

    if (!LanceHasRTreeIndex(dataset, geo_column)) {
      lance_close_dataset(dataset);
      continue;
    }

    // Build the replacement bind data
    auto bind_data = make_uniq<LanceGeoScanBindData>();
    bind_data->dataset_uri = display_uri;
    bind_data->geometry_column = geo_column;
    bind_data->bbox = bbox;

    // Collect projected columns from the original get node
    auto &col_ids = get.GetColumnIds();
    for (auto &col_id_ref : col_ids) {
      auto col_id = col_id_ref.GetPrimaryIndex();
      if (col_id < lance_table->GetColumns().LogicalColumnCount()) {
        bind_data->columns.push_back(
            lance_table->GetColumns()
                .GetColumn(LogicalIndex(col_id))
                .Name());
      }
    }

    // Populate the Arrow schema for type conversion in Execute
    try {
      PopulateGeoScanSchema(context, dataset, *bind_data);
    } catch (...) {
      lance_close_dataset(dataset);
      continue;
    }
    lance_close_dataset(dataset);

    // Replace the filter+get subtree with lance_rtree_index_scan
    auto col_ids_copy = get.GetColumnIds();
    auto new_get = make_uniq<LogicalGet>(
        get.table_index, GetLanceGeoScanFunction(), std::move(bind_data),
        get.returned_types, get.names);
    new_get->SetColumnIds(std::move(col_ids_copy));

    // If the filter had multiple expressions, keep the non-spatial ones
    // as a filter on top of the new get.
    if (filter.expressions.size() == 1) {
      // Only the spatial predicate, replace the entire filter+get
      node = std::move(new_get);
    } else {
      // Remove the matched spatial predicate, keep the rest
      filter.expressions.erase(filter.expressions.begin() +
                                NumericCast<std::ptrdiff_t>(i));
      filter.children[0] = std::move(new_get);
    }
    return true;
  }

  return false;
}

static void OptimizeRecursive(ClientContext &context,
                              unique_ptr<LogicalOperator> &node) {
  // Bottom-up traversal
  for (auto &child : node->children) {
    OptimizeRecursive(context, child);
  }
  TryOptimizeFilter(context, node);
}

static void
LanceRTreeScanOptimizerFunc(OptimizerExtensionInput &input,
                            unique_ptr<LogicalOperator> &plan) {
  OptimizeRecursive(input.context, plan);
}

// ─── Registration ───────────────────────────────────────────────────────────

void RegisterLanceGeoScan(ExtensionLoader &loader, DBConfig &config) {
  // Register the table function (user-facing for direct invocation)
  TableFunction geo_scan_func = GetLanceGeoScanFunction();
  CreateTableFunctionInfo geo_scan_info(std::move(geo_scan_func));
  loader.RegisterFunction(std::move(geo_scan_info));

  // Register the optimizer extension
  OptimizerExtension rtree_opt;
  rtree_opt.optimize_function = LanceRTreeScanOptimizerFunc;
  config.optimizer_extensions.push_back(std::move(rtree_opt));
}

} // namespace duckdb
