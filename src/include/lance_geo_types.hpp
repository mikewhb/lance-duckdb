// src/include/lance_geo_types.hpp
// Minimal bounding box type for Lance RTree spatial queries.
// Zero dependency on duckdb-spatial's geometry library.
// Uses f64 to match Lance's internal BoundingBox precision.
#pragma once

#include <cstddef>

namespace duckdb {

/// Axis-aligned bounding box with f64 precision.
/// Matches Lance's internal BoundingBox (lance-geo/src/bbox.rs).
/// duckdb-spatial uses Box2D<float> (f32); we use f64 to avoid precision loss.
struct LanceBBox {
  double min_x = 0.0;
  double min_y = 0.0;
  double max_x = 0.0;
  double max_y = 0.0;

  LanceBBox() = default;
  LanceBBox(double min_x, double min_y, double max_x, double max_y)
      : min_x(min_x), min_y(min_y), max_x(max_x), max_y(max_y) {}
};

/// Spatial predicate function names that can be accelerated by Lance RTree.
/// These are the function names registered by duckdb-spatial at runtime.
/// Must match GeoQueryParser::visit_scalar_function() in lance-index.
inline bool IsLanceSpatialPredicate(const char *name) {
  // These names match duckdb-spatial's registered function names (case-sensitive).
  return (name[0] == 'S' || name[0] == 's') &&
         (name[1] == 'T' || name[1] == 't') && name[2] == '_' &&
         (strcmp(name, "ST_Intersects") == 0 ||
          strcmp(name, "ST_Within") == 0 ||
          strcmp(name, "ST_Contains") == 0 ||
          strcmp(name, "ST_Covers") == 0 ||
          strcmp(name, "ST_CoveredBy") == 0 ||
          strcmp(name, "ST_Touches") == 0 ||
          strcmp(name, "ST_Crosses") == 0 ||
          strcmp(name, "ST_Overlaps") == 0);
}

} // namespace duckdb
