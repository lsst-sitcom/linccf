## Periods in Rubin compared to Periods in other surveys

**Overall goal:** Compare periods from periodic objects we found in Rubin ComCam data to other surveys. We can use this for validation and interesting science.

1. Take all periodic objects that Kostya has found in the ComCam data.
2. Find the corresponding objects in the other survey.
3. If the periods are not computed in that survey, run the LombScargle algorithm on the lightcurves to obtain them.
4. Compare the results.
5. Start with Gaia and expand to ZTF, PanSTARRS and DES.

### Feedback

LSDB+nested enabled an efficient and clean workflow so there's not much to report. The pain points I faced (some of which are already known) were:

- It's still very incovenient to have to specify the meta for `reduce` catalog operations;
- When loading a catalog, casting its nested columns to the appropriate `NestedDType` adds a bit of toil. I think that's being taken care of on the nested side;
- It's currently hard to know what unit a column is specified in. I was getting PanSTARRS magnitudes of value 40 and that was because during the flux to magnitude conversion I was using nano-Jansky instead of Jansky. I had to navigate to the PanSTARRS column descriptions page to figure that out;
- It took me longer to come up with the plotting code to get survey light curves side by side than writing the code required to gather all the data. We could probably help with major light curve plotting functionality.

### Daily log

#### Wed, Mar 19

Ultimately, we want to find ALL periodic objects in Rubin and perform this analysis. For this homework I will focus on a very reduced set of 10 known objects. I'll use Rubin `w_2025_10` and find matches in another survey. I'll perform the crossmatch and verify that the objects I get are scientifically relevant (e.g. that the brightness for the objects obtained from each survey are similar to the ones in Rubin).

#### Thu-Fri, Mar 20-21

Created [periodic_ztf](./periodic_ztf.ipynb), where I reused a lot of code from the [periodic_lightcurves](../../../demo_notebooks/periodic_lightcurves.ipynb) demo notebook, in particular for plotting. When working with ZTF (and next with PS1) I won't need to perform the crossmatch since we already have HATS catalogs with the full Rubin x-match results.

#### Mon, Mar 24

Spent quite a bit of time polishing the plotting code, removing outliers and making sure the plotting axes made sense.

#### Tue, Mar 25

I was able to plot the light curves of ZTF vs Rubin! The ones in the Rubin data look great - when folded we can observe the sinusoidal curve - but the ones on ZTF not so much. Decided to try filtering the detections by quality flags but that did not help. Created [period_ps1](./periodic_ps1.ipynb), where I tried to recreate the analysis with PS1. I plotted its multiband light curves against those of Rubin but they are quite noisy!

#### Wed, Mar 26

Debugged why the periods I was getting for both ZTF and PS1 were so odd, and out of range. I am now using astropy's implementation of Lombscargle which allows me to define a frequency grid and search with around a ~10% deviation from the true period. The periods are now so much better (error of ~0.01)!

I was going to have a look at Gaia. Turns out the main catalog does not contain the period estimates. These are in a separate catalog which wasn't ported from hipscat to hats. The script to do hats convertion already exists [here](https://github.com/delucchi-cmu/hipscripts/blob/e4f1bc683238a35eb8becc007912cc50334c8fb3/epyc/hats_conversion/create_epoch_photometry_hats.py#L51).
I executed it but I ran into issues with Dask.

#### Thu, Mar 27

Taking a final stab at the comparison with Gaia. After discussing it with Doug, I am following his strategy. I am going to load the variability catalogs for VRRLyrae and Cepheids as they have very-well defined periods already specified.

Well, only 8 out of 10 objects had a match in Gaia. Of those, we only got period estimates for 2 of them. I checked the classification for these objects on Vizier and it makes sense - they are VRRLyrae, and for those we had period information. The remaining ones are eclipsing binaries. I'll try to find the missing periods on the Variability Star Catalog (VSX).

#### Fri, Mar 28

Last minute changes. Fixed a bug found on the period calculation.
