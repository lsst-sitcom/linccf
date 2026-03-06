# 5. Alert-to-Spectra Discovery Pipeline: Identifying Strong H-alpha Emitters with LSDB Crossmatches. 

The goal is to find strong H-alpha emitters in alert data. Use active Rubin Alerts (determine on your own how to get alerts; one idea could be Fink bulk-download service but other options exist). Crossmatch your sample with the LSDB to Hugging Face SDSS spectra. If you are unable to find crossmatches due to the small size of Rubin alerts, use ZTF alerts as a fallback. Once you have found crossmatches, determine the strength of H-alpha emission (the total area under the curve over about 20 nanometers around H-alpha; modify if needed). Create an exact definition of ``strength of Halpha``.  Looking at alerts, count the fraction of H-alpha emitters (for instance, flux in H-alpha region being 5 sigma above noise) for red (magnitude in g - magnitude in r is between 1..5 and 2; use mean object science magnitude) and blue (magnitude in g - magnitude in r is between 0 and 0.5) objects. Are red or blue alert objects more likely to be H-alpha emitters? 

## Definition of done:
- Confirm at least 100 valid crossmatches after quality cuts.
- H-alpha strength definition is explicit and reproducible.
- Color-based fractions result reported with caveats.
