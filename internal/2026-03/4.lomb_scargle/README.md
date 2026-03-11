# 4. Lomb–Scargle at Production Scale: AI-Generated Pipelines vs Native LSDB/HATS Workflow. 

Ask ChatGPT and Claude how to measure the period on ~100 RR Lyr lightcurves in Rubin data, cross-matched with the VSX catalog (HATS-on-the-fly version for the LSDB pipeline). Implement the code as suggested by the agents. Prompt them to work on DP2 even though it is not yet publicly available. You don't have to accept the first solution, work with agents until the proposal seems reasonable. Store the final prompt that you have used (ask ChatGPT/Claude to give you summary of the current prompt they are using). Implement a similar workflow using LSDB/HATS. You are allowed to prompt agents on how they would do it with LSDB/HATS. Restrict algorithm family to Lomb-Scargle.  
Compare among these 3 methods:
- performance (execution speed), 
- memory usage
- and ease of use.

## Definition of done:
- Three runnable implementations with the same dataset slice.
- Comparative table for speed/memory/ease-of-use.


## Claude Notes:
In this assignment, I didn't feel that there was a realistic pathway for Claude/another AI to get to a runnable implementation, so as the main deliverable on this front I'll try to just explain the steps I took, and the limitations I saw.
### Iteration 1
I gave it a pretty open ended prompt to begin:

*Can you write a small script in python that measures the periods of 100 RR Lyrae lightcurve using Rubin Data Preview 2 Data, crossmatched with the VSX catalog? For period measurement, use astropy's lomb-scargle implementation.*

It gave me back a notebook (script first technically) that looked a bit like a Rubin tutorial notebook, going in the direction of a DP2 TAP service. A lot of the tap setup failed, when I tried to run things. It claims I was missing a token, but I'm not sure that was explicitly required to get the rubin tap service actually working. Tough to say as I tried to run their notebooks directly and was running into tap issues as well. I didn't push on this too much, because DP2 data wouldn't have been available through this regardless.

Another note is that Claude opted to grab 100 random RR Lyrae from vizier, which is reasonable, but fully neglected to consider overlap with the DP2 field and any filtering steps that would remove objects from this initial grab.

The actual analysis steps seemed okay at first glance (which is also AI's specialty), but I didn't work with it at that stage due to the data loading issues up front.

### Iteration 2

Next, I made it aware of the HATS DP2 pilot:

*Let's say that we were on a cluster with local access to a DP2 pilot, with this location: '/sdf/data/rubin/shared/lsdb_commissioning/hats/v30_0_0_rc2/object_collection'*

*Please use this directly, instead of going through the TAP*

It took the hint well here and pivoted to LSDB for the DP2 loading, but stuck with vizier for VSX loading. It (with some prompting) made note to try to load VSX RR Lyrae within the DP2 field, but the 186 candidates it found only resulted in a few matches when crossmatching with LSDB, through from_dataframe, both of which only had one observation. It then tried to unpack the nested columns for anaylsis. In hindsight I could have pushed on this more to try to use `map_rows` and nested-pandas operations more here, but was more focused on the failed sample collection.
### Iteration 3
For iteration 3, I let Claude know that VSX is also available through HATS and to use LSDB for both catalogs, which it did. However, the filtering it did up front of VSX was to both find things only in a subset of the footprint and that have a known period, which failed to return any objects. It looks close to right, but maybe just need a few query tweaks, along with not requiring a period from VSX to be processed.
### Iteration 4
Finally, because it wasn't finding any objects, I tried to have claude lean more on a full crossmatch to find any potential matches. However, this is where we entered LSDB performance issues, and I cycled off the AI side and really dove into LSDB performance issues.


