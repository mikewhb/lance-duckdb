# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(lance
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# Load the spatial extension (from submodule) with heavy dependencies disabled.
# Core functionality (GEOMETRY type, ST_* functions, RTree index, GeoArrow) is retained.
set(SPATIAL_USE_GEOS ON CACHE BOOL "Enable GEOS for lance build" FORCE)
set(SPATIAL_USE_PROJ OFF CACHE BOOL "Disable PROJ for lance build" FORCE)
set(SPATIAL_USE_GDAL OFF CACHE BOOL "Disable GDAL for lance build" FORCE)
set(SPATIAL_USE_NETWORK OFF CACHE BOOL "Disable network for lance build" FORCE)
set(SPATIAL_USE_SHAPEFILE OFF CACHE BOOL "Disable shapefile for lance build" FORCE)
set(SPATIAL_USE_OSM OFF CACHE BOOL "Disable OSM for lance build" FORCE)
set(SPATIAL_USE_MVT OFF CACHE BOOL "Disable MVT for lance build" FORCE)

duckdb_extension_load(spatial
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}/duckdb_spatial
    INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/duckdb_spatial/src/spatial
    LOAD_TESTS
)

# json extension is required by spatial
duckdb_extension_load(json)

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)
