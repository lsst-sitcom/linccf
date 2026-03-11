# 4. Lomb–Scargle at Production Scale: AI-Generated Pipelines vs Native LSDB/HATS Workflow. 

Ask ChatGPT and Claude how to measure the period on ~100 RR Lyr lightcurves in Rubin data, cross-matched with the VSX catalog (HATS-on-the-fly version for the LSDB pipeline). Implement the code as suggested by the agents. Prompt them to work on DP2 even though it is not yet publicly available. You don't have to accept the first solution, work with agents until the proposal seems reasonable. Store the final prompt that you have used (ask ChatGPT/Claude to give you summary of the current prompt they are using). Implement a similar workflow using LSDB/HATS. You are allowed to prompt agents on how they would do it with LSDB/HATS. Restrict algorithm family to Lomb-Scargle.  
Compare among these 3 methods:
- performance (execution speed), 
- memory usage
- and ease of use.

## Definition of done:
- Three runnable implementations with the same dataset slice.
- Comparative table for speed/memory/ease-of-use.
