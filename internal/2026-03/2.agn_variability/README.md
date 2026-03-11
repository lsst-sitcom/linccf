# 2.  Short-Timescale AGN Variability in DP2: Crossmatch, GP Modeling, and Bandwise Ranking 

Cross-match Gaia AGN HATS-on-the-fly catalog with DP2. Select 60 objects: top-10 objects with the densest average cadence in each band (u/g/r/i/z/y) ( average cadence is inverse of total_length_of lightcurve_in_days/Number_of_observations).  Fit variability model with EzTaoX. Based on the result of the fitting answer:

- Which band is the most variable on ~10-day scale?
- Which AGN is the most variable in each of u/g/r/i/z/y bands? 

Plot the lightcurve and on the same plot the GP fit for the most variable AGN in u/g/r/i/z/y band. In your judgment, is this physical variability, or the variability due to instrumental effects?

## Definition of done:

- 60-object selection reproducible.
- Model fit completes and produces interpretable outputs.
- Required plots (6 AGN, in u/g/r/i/z/y) included.
- Variability conclusions are justified
