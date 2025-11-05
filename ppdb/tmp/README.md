## Prompt Products Database (PPDB)

Scripts to hatsify data from PPDB.

This initial work follows a similar approach to that of [DASH](../dash/):

| Stage   | Description                                                                            |
| ------- | -------------------------------------------------------------------------------------- |
| Stage 1 | Import the new catalog data (objects, source and forced source) into HATS.             |
| Stage 2 | Apply post-processing (add magnitudes, downcast non-positional/time columns).          |
| Stage 3 | Nest new sources and forced sources for each object, sorting by timestamp.             |
| Stage 4 | Concatenate existing collection with the new one. Generate index catalog on object ID. |

### Latest collection available on USDF at:

```
/sdf/data/rubin/shared/lsdb_commissioning/hats/PPDB/dia_object_collection
```
