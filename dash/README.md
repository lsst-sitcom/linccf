# DASH pipeline

## Setting up the environment

This pipeline requires IDAC access and is normally run on USDF SLAC
nodes.  It cannot be run on the login node, because login nodes are
not for that purpose.  From such a login node, you need to log into an
interactive node.  It is *highly recommended* to use `tmux` so that
you can return to the specific interactive node to check on progress.
The pipeline typically takes at *least* ~5h and can take closer to
~15h with `dp2_prep`.

You can run this pipeline on an interactive node, but use of more than
30 cores is frowned upon, as well as very long-lived jobs.  It is
recommended to start a *reserved* node from the interactive node, like
this:

```shell
srun --pty --exclusive --nodes=1 --time=48:00:00 \
     --partition=milano --account=rubin:commissioning bash
```

After resources have been allocated, the node is yours to use for as
long as the given `--time` argument.  However, it will also be
immediately terminated if you exit the shell.  To avoid this, do not
exit from the reserved node, but do a `tmux` detach.  Your route into
the reserved node should look something like this:

    login-node -> tmux -> interactive-node -> reserved node

This way, it's easy to return to your login node (make sure it's the
same one each time), `tmux attach`, and be right back at your reserved
node interactive session.

When you first start the reserved node, set up the LSST environment:

```shell
source /sdf/group/rubin/sw/loadLSST.sh
setup lsst_distrib
```

and then set the following environment variables according to the
values in the "Description" section of the JIRA that is associated
with the particular weekly release.

```shell
export REPO=dp2_prep
export INSTRUMENT=LSSTCam
export RUN=20250417_20250921
export VERSION=w_2025_49
export COLLECTION=DM-53545
export OUTPUT_DIR=/sdf/data/rubin/shared/lsdb_commissioning
```

For this example, [this is the
JIRA](https://rubinobs.atlassian.net/browse/DM-53545).  In that
example, you can find:

```
repo = dp2_prep
collection = LSSTCam/runs/DRP/20250417_20250921/w_2025_49/DM-53545
```

and from that, you can get `REPO`, `INSTRUMENT`, `RUN`, `VERSION`, and
`COLLECTION`.  Note that `OUTPUT_DIR` does not vary from week to week.

## Executing and monitoring the pipeline

The pipeline can be executed in two different ways.

### 1. Automated execution

Make sure you give the `00-run.sh` script permission to be executed:

```shell
chmod +x 00-run.sh
```

With these variables set, execute the script in the background, saving
both output and error to `nb.out`.  This allows you to keep monitoring
the job interactively.

```shell
./00-run.sh \
    --INSTRUMENT $INSTRUMENT \
    --REPO $REPO \
    --RUN $RUN \
    --VERSION $VERSION \
    --COLLECTION $COLLECTION \
    --OUTPUT_DIR $OUTPUT_DIR >& nb.out &
```

This will trigger a sequential execution of all pipeline stages. The
resulting *output* Jupyter notebooks will be stored under
`outputs/$VERSION`.  There will also be a log file with the runtime
details at `outputs/$VERSION/runtimes.tsv`.  (Note that this log file
will only be complete if the `00-run.sh` script finishes without any
fatal errors.)

Monitoring the ongoing process:

```shell
tail nb.out
cat outputs/$VERSION/runtimes.tsv
top -U $USER
```

### 2. Interactive execution

If the fully automated execution fails, you can resume it by rerunning
the notebook representing the stage that failed.  Once you find out
what has broken, if something is unexpected about the upstream data
files, you can reach out to the channel `#dm-algorithms-pipelines` at
the Rubin Observatory Slack.  If it's something you can fix or work
around in the notebook, edit it there.

At this point, there is no way to resume the overall execution, but
you can step through each stage individually.

Ensure that the environment variables are set, as above:

```shell
export REPO=dp2_prep
export INSTRUMENT=LSSTCam
export RUN=20250417_20250921
export VERSION=w_2025_49
export COLLECTION=DM-53545
export OUTPUT_DIR=/sdf/data/rubin/shared/lsdb_commissioning
```

and you can then rerun the affected stage.  For example, suppose it's
the Import stage that broke, and after fixing the problem, you want to
rerun it:

```shell
jupyter nbconvert --to notebook --execute 03-Import.ipynb \
                  --output outputs/$VERSION/03-Import.ipynb \
		  --log-level=CRITICAL >& nb.out &
```

This does oblige you to follow the same process for every stage that
follows (04, 05, etc. in the above example).

### 3. Interactive inspection of notebook stages

In some cases, you may want or need to work with one of the affected
notebooks interactively.  You can run Jupyter right from within the
processing environment:

Run Jupyter from the same shell:

```shell
jupyter notebook --no-browser --port=8769
```

The notebook is running all the way into the reserved node.  Before
you can successfully open the provided link (which will be to
`localhost`), you'll need to bring this port back to your machine via
a multi-jump tunnel.  This requires the hostnames of your login,
interactive, and reserved nodes.  Suppose they are `sdflogin003`,
`sdfiana004`, and `sdfmilan005` respectively.  (You can do this in a
separate terminal to establish the tunnel; it doesn't have to be the
same one as you originally used to log in.)

```shell
ssh -J $USER@sdflogin003.slac.stanford.edu,$USER@sdfiana004 \
    -L 8769:localhost:8769 \
    $USER@sdfmilan005
```

From there you can browse and run the input notebooks directly, or
browse the ones in `outputs` to examine cells for warnings and
errors.
