# 1. DP2 Epoch Propagation at Scale: Reproducing and Quantifying DP1→DP2 Gaia Differences. 

Port DP1 vs Gaia DR3 epoch propagation notebook to DP2. Part of the notebook includes large crossmatch. Write the results down to disk - do not cross the threshold of 5 Gb of data (This is because we are only working with ⅙ of the data, and we want to make sure this is feasible with the full DP2 dataset). If needed, reduce the number of columns, and consider how to reduce the number of crossmatches without comprising scientific result. Compare your result to the result in the DP1 vs GAIA DR3 notebook. Answer by how much is the effect due to epoch propagation larger in DP2 than it was for DP1. To do this compare median values for DP1 and DP2 (median measure of the values shown in Section 9 in the documentation notebook).


## Definition of done:

- Crossmatch output exists as HATS catalog.
- Final saved HATS catalog is <= 5 GB.
- There is a quantitative result for the change between DP1 and DP2 result
