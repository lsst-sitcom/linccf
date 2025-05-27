# DASH pipeline

The pipeline can be executed in two different ways.

### 1. Automated execution

Make sure you give the `00-run.sh` script permission to be executed:

```
chmod +x 00-run.sh
```

Execute it with the desired VERSION and COLLECTION. Example:

```
./00-run.sh \
    --INSTRUMENT LSSTComCam \
    --REPO /repo/dp1 \
    --RUN DP1 \
    --VERSION w_2025_10 \
    --COLLECTION DM-49359 \
    --OUTPUT_DIR /sdf/data/rubin/shared/lsdb_commissioning
```

This will trigger a sequential execution of all pipeline stages. The output Jupyter notebooks will be stored under `outputs/{VERSION}`. There will also be a log file with the runtime details.

### 2. Interactive execution

Export the arguments as environment variables. Example:

```
export INSTRUMENT=LSSTComCam
export REPO=/repo/dp1
export RUN=DP1
export VERSION=w_2025_10
export COLLECTION=DM-49359
export OUTPUT_DIR=/sdf/data/rubin/shared/lsdb_commissioning
```

Run Jupyter from the same command-line:

```
jupyter notebook --no-browser --port=8769
```

Go through each notebook. Connect to the Jupyter kernel using the server URL provided by the previous command.
