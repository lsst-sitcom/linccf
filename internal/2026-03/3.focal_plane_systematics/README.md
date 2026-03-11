# 3. Focal-Plane Systematics Check: Edge vs Center Detector Flux-Residual Performance in DP2 

Compute mean fluxes over forced sources and compute relative difference from object flux using: (mean_source_flux - object_flux) / sqrt(mean_source_fluxerr^2 + object_fluxerr). Remove any measurement with any kind of flag. Include every lightcurve per band with more than 10 observations per band, after filtering for flags.
Plot:
- histogram of this value
- mean and std over detectors.

Based on this information, is the performance of the pipelines better for central detectors than for those on the edge of the focal plane (Find on your own which detectors are where - Rubin Observatory will provide this information publicly <UNCLE VAL Script>). Define a reasonable definition of what ``better performance`` means.

## Definition of done:
- Formula implemented correctly with uncertainty propagation, and every lightcurve in the dataset evaluated
- Both required plots present.
- Center vs edge conclusion linked to your performance criterion.
