// rust/ffi/geo.rs
// Spatial RTree index scan FFI for lance-duckdb.
//
// Strategy: Construct a DataFusion `Expr` equivalent to:
//   st_intersects(<geometry_column>, <bbox_literal>)
// and pass it to `Dataset::scan().filter_expr()`.
//
// Lance's internal `GeoQueryParser::visit_scalar_function()` recognizes
// `st_intersects` and automatically routes through `RTreeIndex::search()`.
// This avoids accessing `RTreeIndex::search_bbox()` which is `pub(crate)`.
//
// The RTree returns `SearchResult::AtMost` (may include false positives),
// so Lance's scanner applies a recheck filter using the real `st_intersects`
// UDF from `geodatafusion`. We use that same UDF (not a stub) so that
// the recheck filter can actually execute.

use std::ffi::{c_char, c_void};
use std::ptr;
use std::sync::Arc;

use datafusion_common::metadata::FieldMetadata;
use datafusion_common::ScalarValue;
use datafusion_expr::{Expr, ScalarUDF};
use geo_types::{coord, Rect};
use geoarrow_array::builder::RectBuilder;
use geoarrow_array::GeoArrowArray;
use geoarrow_schema::{Dimension, GeoArrowType, RectType};

use crate::error::{clear_last_error, set_last_error, ErrorCode};
use crate::scanner::LanceStream;

use super::types::StreamHandle;
use super::util::{cstr_to_str, dataset_handle, optional_cstr_array, FfiError, FfiResult};

/// Perform a spatial range query using the Lance RTree index.
///
/// Internally constructs `st_intersects(geometry_column, <bbox_literal>)` and
/// passes it to `Dataset::scan().filter_expr()`, which triggers Lance's
/// GeoQueryParser -> RTreeIndex::search() path.
///
/// Returns NULL on error; call `lance_last_error_message()` for details.
#[no_mangle]
pub unsafe extern "C" fn lance_create_rtree_scan_stream(
    dataset: *mut c_void,
    geometry_column: *const c_char,
    min_x: f64,
    min_y: f64,
    max_x: f64,
    max_y: f64,
    columns: *const *const c_char,
    columns_len: usize,
) -> *mut c_void {
    match create_rtree_scan_stream_inner(
        dataset,
        geometry_column,
        min_x,
        min_y,
        max_x,
        max_y,
        columns,
        columns_len,
    ) {
        Ok(stream) => {
            clear_last_error();
            Box::into_raw(Box::new(stream)) as *mut c_void
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            ptr::null_mut()
        }
    }
}

fn create_rtree_scan_stream_inner(
    dataset: *mut c_void,
    geometry_column: *const c_char,
    min_x: f64,
    min_y: f64,
    max_x: f64,
    max_y: f64,
    columns: *const *const c_char,
    columns_len: usize,
) -> FfiResult<StreamHandle> {
    let handle = unsafe { dataset_handle(dataset)? };
    let geo_col = unsafe { cstr_to_str(geometry_column, "geometry_column")? };

    // Step 1: Build a GeoArrow RectArray representing the query bounding box.
    // This is what Lance's `extract_bounding_boxes()` expects to receive when
    // it processes the `GeoQuery::IntersectQuery.value` ScalarValue.
    let rect_type = RectType::new(Dimension::XY, Default::default());
    let mut builder = RectBuilder::with_capacity(rect_type.clone(), 1);
    builder.push_rect(Some(&Rect::new(
        coord! { x: min_x, y: min_y },
        coord! { x: max_x, y: max_y },
    )));
    let rect_array = builder.finish();

    // Convert to an Arrow array and extract a ScalarValue from the first element.
    let arrow_array = rect_array.into_array_ref();
    let scalar_val = ScalarValue::try_from_array(&arrow_array, 0).map_err(|e| {
        FfiError::new(
            ErrorCode::DatasetScan,
            format!("failed to create ScalarValue from bbox RectArray: {e}"),
        )
    })?;

    // Step 2: Build the Arrow field with GeoArrow extension metadata.
    // GeoQueryParser extracts metadata from Expr::Literal's FieldMetadata,
    // then attaches it to a Field so that `extract_bounding_boxes()` can
    // interpret the ScalarValue as a GeoArrow geometry.
    let geo_field = GeoArrowType::Rect(rect_type).to_field("_geo", false);

    // Step 3: Construct the DataFusion expression:
    //   st_intersects(<geo_col>, <bbox_literal>)
    //
    // Use the real `Intersects` UDF from geodatafusion (not a stub) so that:
    //   a) GeoQueryParser::visit_scalar_function() matches the function name
    //   b) The recheck filter (needs_recheck=true for RTree) can actually execute
    let udf = Arc::new(ScalarUDF::new_from_impl(
        geodatafusion::udf::geo::relationships::Intersects::new(),
    ));
    let col_expr = Expr::Column(datafusion_common::Column::new_unqualified(geo_col));
    let lit_metadata: Option<FieldMetadata> = if geo_field.metadata().is_empty() {
        None
    } else {
        Some(FieldMetadata::new_from_field(&geo_field))
    };
    let lit_expr = Expr::Literal(scalar_val, lit_metadata);
    let filter_expr = Expr::ScalarFunction(datafusion_expr::expr::ScalarFunction {
        func: udf,
        args: vec![col_expr, lit_expr],
    });

    // Step 4: Set up projection
    let projection = unsafe { optional_cstr_array(columns, columns_len, "columns")? };

    // Step 5: Execute the scan with the spatial filter
    let mut scan = handle.dataset.scan();

    if !projection.is_empty() {
        scan.project(&projection).map_err(|e| {
            FfiError::new(ErrorCode::DatasetScan, format!("scan project: {e}"))
        })?;
    }

    scan.filter_expr(filter_expr);
    scan.scan_in_order(false);

    let stream = LanceStream::from_scanner(scan).map_err(|e| {
        FfiError::new(ErrorCode::StreamCreate, format!("stream create: {e}"))
    })?;

    Ok(StreamHandle::Lance(stream))
}
