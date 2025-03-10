# DASH: DRP Afterburner for Super HATS

The LINCC Frameworks team has been leading the effort to create interoperable
HATS catalogs, usable by the LSDB analytics library. DASH is our attempt to take
LSST observations that have been processed by Rubin DM DRP (Data Management
Data Reduction Pipeline) into catalogs of objects and sources, and collate them
into HATS-partitions parquet files on USDF.

This is not a simple copy of the files, however, as we add value along the way.

## Nested output

There are three primary output tables:

* `diaObject_lc`
    * Combines data from three Butler tables: `diaObjectTable_tract`,
      `diaSourceTable_tract`, `forcedSourceOnDiaObjectTable`
    * "Nests" the diaSourceTable and forcedSourceOnDiaObject tables as structured
        lists under each object row. Sources within an object are sorted by detection time.
    
* `object_lc`
    * Combines data from two Butler tables: `objectTable`, `forcedSourceTable`
    * "Nests" the forcedSourceTable table as structured lists under each object row. Sources
      within an object are sorted by detection time.

* `source`
    * Contains data from the Butler table: `sourceTable`
    * It is a flat table with the sources detected in science imaging. It is an independent
      table because there is no association between objects of the `objectTable` and the
      existing sources.

## Butler post-processing

We read the catalog data from the same parquet files that Butler uses.
We add value as we're reading those files, and in post-processing shortly after.

* `object` table - limit to only ~100 columns (from original TODO how many?)
* Remove any rows that have nan or null ra/dec (or coord_ra/coord_dec) values. 
* We append columns to the results that correspond to Butler dimensions.
  In practice, this means that we retain information on tract and patch that
  are otherwise not stored either in Butler's backing parquet files, or typically
  returned in a call to get a single data table from the Butler (but is inferable
  from the Butler data reference).
    * 'diaObjectTable_tract':["tract"],
    * 'diaSourceTable_tract':["tract"],
    * 'forcedSourceOnDiaObjectTable':["patch", "tract"],
    * 'objectTable':["patch", "tract"],
    * 'sourceTable':["band","day_obs","detector","physical_filter","visit"],
    * 'forcedSourceTable':["patch", "tract"]
* For all flux (and fluxErr) columns, use the known zero-point to calculate 
    corresponding mag (and magErr) in nJy:
    * diaSource: ["psf", "science"]
    * diaForcedSource: ["psf"]
    * object: ['u_psf', 'u_kron', 'g_psf', 'g_kron', 'r_psf', 'r_kron', 'i_psf', 'i_kron', 'z_psf', 'z_kron', 'y_psf', 'y_kron']
    * source: ["psf"]
    * forcedSource: ["psf"]
* Add midpointTai in MJD to source-like tables, where only `visitId` is stored 
    (source, diaForcedSource, forcedSource). We do this with an offline join to the
    `visitTable`.

## Validation

Once catalogs have been created, we perform some science validation on the catalog data. This is split into two notebooks:
* [Basic Statistics](../dash/06.a-Basic%20Statistics.ipynb) 
    * Gathers global min and max values for all columns, and reports on any that 
        are outside acceptable ranges (e.g. ra values must be (0.0, 360.0))
    * Counts the number of null or nans in all columns
    * Prints other basic counts, like number of rows, number of columns,
        number of partition files, and sky coverage in % of the sky.
* [By Field](../dash/06.b-ByField.ipynb)
    * Performs more detailed verification on the datasets, using LSDB to inspect 
        leaf parquet files, using the six fields used in comcam data 
        (ECDFS, EDFS, Rubin_SV_38_7, Rubin_SV_95_-25, 47_Tuc, Fornax_dSph)
    * Compute weighted mean, grouping by field and band, for all four source types












## Reference Materials


* LSDB ([on GitHub](https://github.com/astronomy-commons/lsdb)) 
  ([on ReadTheDocs](https://lsdb.readthedocs.io/en/stable/))
  ([on arXiv](https://ui.adsabs.harvard.edu/abs/2025arXiv250102103C))
* HATS ([on GitHub](https://github.com/astronomy-commons/hats))
  ([on ReadTheDocs](https://hats.readthedocs.io/en/stable/))
* nested-dask ([on GitHub](https://github.com/lincc-frameworks/nested-dask)) 
  ([on ReadTheDocs](https://nested-dask.readthedocs.io/en/stable/))
* nested-pandas ([on GitHub](https://github.com/lincc-frameworks/nested-pandas)) 
  ([on ReadTheDocs](https://nested-pandas.readthedocs.io/en/stable/))

Other useful material:
- Ex JIRA ticket: https://rubinobs.atlassian.net/browse/DM-48478
- https://github.com/LSSTScienceCollaborations/StackClub/tree/master

