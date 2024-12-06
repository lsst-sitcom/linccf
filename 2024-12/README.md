# Rubin project work items for LSDB/HATS project [December 2024]

Steps copied from original document [here](https://docs.google.com/document/d/1wEbmDjDT9XSby6KjG-1s7L8PG37nujf9sAV4HV2KEIY/edit?tab=t.0)

## Project 1. Operational rehearsal 4 data

### Subproject 1

* Load truth data in a catalog - [notebook here](https://github.com/lsst-sitcom/notebooks_dia/blob/DM-46125/Intro.ipynb)
* Notebook loads only 2 pixels - load all pixels.
* Notebook loads only fluxes for 2 pixels - load all fluxes
* Transform fluxes to magnitudes using `flux_to_magnitude` custom function available
* Join these two tables

### Subproject 2

* Load simulated data in a catalog - we should be able to access PPDB
* If we can not make PPDB access a reality we can go via butler. Perhaps we should try that anyhow?

### Subproject 3

* Crossmatch these two catalogs. Report completeness and purity

### Subproject 4

* Run LS periodogram on the simulated data catalog, on full lightcurves (so on ForcedSources!). 
* Other notebooks might be of some help (e.g., [here](https://github.com/lsst-sitcom/notebooks_dia/blob/DM-46125/DM-46126.ipynb))
* Extra points for using nested packages.

### Notes on DRP graph

https://tigress-web.princeton.edu/~lkelvin/pipelines/current/drp_pipe/LSSTComCam/DRP/

https://tigress-web.princeton.edu/~lkelvin/pipelines/2024/w_2024_37/ap_pipe/LSSTComCamSim/ApPipe/pipeline_ap_pipe_LSSTComCamSim_ApPipe.pdf

## Project 2 - ComCam diaObjects

* Load (put in a catalog) Comcam difference image processing (diaObjects and diaSources) 
* (using data reduction pipeline) data using the following information in [this Slack message ](https://rubin-obs.slack.com/archives/C07QM71SZ5J/p1733245089159129)
* Find more hints in the notebook [here](https://github.com/lsst-sitcom/notebooks_dia/blob/main/Comcam_init_analysis.ipynb)
* [This](https://rubin-obs.slack.com/archives/C07QM7144BW/p1732920073930759?thread_ts=1732914990.931929&cid=C07QM7144BW) message gives what observations are used.
* If you want to look at the data use [this notebook](https://github.com/lsst-sitcom/notebooks_dia/blob/main/comcam_drp_dia_inspection.ipynb)

## Project 3 - ComCam sources

* Load  (put in a catalog) all of the sources that we the survey has detected. Use the same resources as above, but load Sources and Objects.


## Additional Notes

Food for thought: Sizes that we expecting explained in the [message here](https://rubin-obs.slack.com/archives/C07TXQUAXUZ/p1732902598691429?thread_ts=1732892665.373009&cid=C07TXQUAXUZ)

