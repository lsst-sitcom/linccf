# DASH pipeline

The pipeline can be executed in two different ways.

### 1. Automated execution

Make sure you give the `00-run.sh` script permission to be executed:

```
chmod +x 00-run.sh
```

Execute it with the desired DRP_VERSION and COLLECTION_TAG. Example:

```
./00-run.sh --DRP_VERSION w_2025_10 --COLLECTION_TAG DM-49359
```

This will trigger a sequential execution of all pipeline stages. The output Jupyter notebooks will be stored under `outputs/{DRP_VERSION}`. There will also be a log file with the runtime details.

### 2. Interactive execution

Export DRP_VERSION and COLLECTION_TAG as environment variables. Example:

```
export DRP_VERSION="w_2025_10"
export COLLECTION_TAG="DM-49359"
```

Run Jupyter from the same command-line:

```
jupyter notebook --no-browser --port=8769
```

Go through each notebook. Connect to the Jupyter kernel using the server URL provided by the previous command.
