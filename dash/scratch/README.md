## Notes from 2025-01-22

starting hats tables:
* diaobject
* dia source (visit id, NOT MJD)
* object
* forced source (visit id, NOT MJD)

what we want: object with:
* limited to 20-50 columns
* brightness in magnitude
* source light curve
   * (DONE)visit ID AND MJD (DONE)
   * brightness in magnitude
   * (DONE)"diaObjectId"...
* force source light curve
   * (DONE) visit ID AND MJD
   * brightness in magnitude
   * (maybe combined into a single light curve)

what we need to do:
* list of 50 columns from scientists (start with ra,dec, id, mag_r)
* filter by validity
* get those starting tables
* (DONE (for now - will not scale well to 100M visits)) visit id to MJD look up (could maybe do one table per final healpix?)
