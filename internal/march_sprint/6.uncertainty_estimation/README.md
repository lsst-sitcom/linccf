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

So. How do I filter out rows where the lightcurves are too short? The painfully obvious things I'm doing aren't working.

The silly brute force method seems to work ok. So I'm going to go with that for a little while.

```
obs_mask = [len(n) > 1 for n in field_frame["diaSource"]]
field_frame = field_frame[obs_mask]
```

But trying this on THE ENTIRE `diaObject` catalog is taking fooooorrrreeevvveeerrr. Sure wish we
could have a progress bar, like civilized people.

But also, the distribution of lightcurve lengths is whack.

![alt text](image-1.png)

Look at that! 99%+ of diaObjects have lightcurves that have JUST ONE observation. Neven says these are "fake detections".
This might not be a very useful data set. Maybe I should have started with `object_lc` instead...

### Thursday

What happened to Wednesday?!

USDF RSP won't start a notebook instance for me. So I'm going to wait an hour and see if that clears ... something. 
That worked last time.