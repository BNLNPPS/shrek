# Quick start for SHREK

## Cloning the repository and initial setup

First clone the repository and change into the shrek
directory

```
$ git clone https://github.com/klendathu2k/shrek
$ cd shrek
```

Next, you'll want to modify the site configuration file, using
your favorite text editor

```
$ edt shrek/config/site.yaml
```

For sPHENIX usesr, there are two fields which need to be modified 
in order to get the system running:

### Shrek/submissionPrefix points to a directory where shrek will create

Shrek/submissionPrefix points to a directory where shrek will create
the job submission documents for PanDA and gather any supporting files
needed to run the job.  You should create a fresh directory and initialize 
a local git repository inside of it.

```
$ mkdir ../my-submission-area
$ cd ../my-submission-area
$ git inti
$ cd -
```

Under "Shrek" there is a "submissionPrefix" option.  This points to
a directory

### Donkey/scope

This is the target scope used by the `donkey` application to discover datasets.
It is typically user.${USER}

## Running tests

Next, you should run the test suite to confirm codes are setup properly.

```
$ pytest -v tests/
```

## Setting up handy aliases

```
$ alias donkey='shrek/scripts/watchRucioForNewDatasets.py'
$ alias shrek='shrek/scripts/submitWorflowToPanDA.py'
```

## Running test jobs with shrek

Issuing the following command will run the shrek processor on the simple chain
workflow, creating the directory "sP00a-simple" in your submission area, and
assembling the job submission scripts there.  There will be no interaction 
with the PanDA server.

```
$ shrek --no-submit --no-check --no-handshake --tag sP00a-simple tests/simple-chain/*.yaml
```

There are three flags -- `no-submit`, `no-check` and `no-handshake` -- which instruct shrek not
to have any interaction with the PanDA servers.  On a typical call, shrek will begin by
trying to send a handshake message to the PanDA server.  PanDA will either respond that
it is ready to accept jobs, or request authentication.  (Or possibly fail at this point).

Run 

```
$ shrek --no-submit --no-check --handshake --tag sP00a-simple tests/simple-chain/*.yaml
```

to make sure you are authenticated with the PanDA server, following any instructions
you recieve.  [You could omit the `--handshake` option as it is the default].

Next we want to validate the workflow.  Run

```
$ shrek --no-submit --check --handshake --tag sP00a-simple tests/simple-chain/*.yaml
```

The PanDA server will return true if the CWL document is properly structured and able
to be accepted.  This is not a guarentee that the code will run, but it does demonstrate
that shrek has converted the job definition files under `tests/simple-chain` into something
which PanDA can handle.  [Again, the `--check` flag is enabled by default and could be
omitted.]

If all goes well, shrek should report that PanDA considers the workflow to be valid,
and provides instructions for how to submit the job by hand.

Finally let's submit the job to PanDA via shrek.  Issue

```
$ shrek --submit --tag sP00a-simple tests/simple-chain/*.yaml
```

As mentioned above, the `handshake` and `check` options are on by default, so we have
omitted them from this example.  The `submit` option, however, is not on by default.
So we explicitly tell shrek to submit the job.

If all goes well, you'll see a URL pointing you to the job monitoring page for your run.