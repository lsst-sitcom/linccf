## Prompt Products Database (PPDB)

Scripts to "hatsify" difference imaging (DIA) catalog data from PPDB.

### Daily stage

Each night, increment the existing catalog with new data:

- Import the new catalog data (diaObject, diaSource and diaForcedSource) separately into HATS.

- Apply post-processing (filter objects by validity start, add magnitudes from fluxes). 

- Nest new sources and forced sources in each object, sorting them by timestamp.

- Write new daily parquet files, update relevant HATS properties and skymaps.

This stage avoids rewriting pre-existing files, minimizing I/O.

```
dia_object_lc/
|__ dataset/
    |__ Norder=2/
    |   |__ Dir=0/
    |       |__ Npix=102/
    |           |__ 2025-11-19.parquet
    |           |__ 2025-11-18.parquet
    |           |__ ...
    |           |__ Npix=102.parquet (past data, aggregated)
    |__ Norder=4/
    |   |__ Dir=0/
    |       |__ Npix=1733/
    |           |__ 2025-11-19.parquet
    |           |__ 2025-11-18.parquet
    |           |__ Npix=1733.parquet (past data, aggregated)
    |__ .../
    |__ _common_metadata (constant)
|__ partition_info.csv (updated)
|__ hats.properties (updated)
|__ skymap.fits (updated)
```

### Weekly stage

Every so often, reprocess the catalog, aggregating the pixel data:

- Deduplicate object rows (keeping the latest diaObject-level data for each object).

- Merge each object's source and forced source.

- Reimport according to a more balanced threshold argument. 

- Generate collection with margin cache and index catalog.

### Possible improvements

When deduplicating object rows, if some of the most recent object-level data is missing and it existed previously, fill those missing (NaN values) with the latest available information.
