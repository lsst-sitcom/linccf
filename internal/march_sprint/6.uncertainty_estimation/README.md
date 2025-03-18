## Description

**Overall goal**: Check that DP1 flux errors are not biased.

For each of the source tables, select all light curves with no obvious variability 
and plot the relative deviation histogram. Remove all datapoints with ANY flags. 
Compare the histogram with the standard distribution.

1. For each type of the photometric product (forced sources, DIA sources, DIA forced sources) 
   and each passband, select all light curves with at least 10 observations and no obvious 
   variability (no points beyond 10-sigma).
2. Find weighted mean flux per object: \
	`wmean_flux = np.average(flux, weights=1/flux_error**2)`
3. Get relative deviations: \
	`rel_dev = (flux - wmean_flux) / flux_error`
4. Plot the distribution of relative deviations, accumulating them over all the objects, 
   as a histogram. Compare with the standard distribution within rel_dev values between -3 and 3.
5. Bonus points for making a statistical test and running the analysis per field and per magnitude bin (for example one-mag-wide).

## Lab notes

### Tuesday, March 18

Goal for today: step 1. Get boring lightcurves.