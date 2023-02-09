# Quick(er) start for SHREK

# Log into the sphnxpro account and execute
```
$ cd shreksys
$ git pull
$ source setup.sh
$ alias shrek='shrek/scripts/submitWorflowToPanDA.py'0
```

# Obtain a current, valid token

Execute the following command.
```
shrek --handshake --tag none tests/simple/top.yaml
```
Shrek will ping the PanDA server, and verify that you have a valid token.
The program will output the following and pause waiting for your input...
...

...
[shrek WARNING] Poking the bear you may be prompted for a new token.   
INFO : Please go to ...................... and sign in. Waiting until authentication is completed    
INFO : Ready to get ID token?    
[y/n]  

Follow the link and authenticate through your ID provider (BNL/SDCC) with
your user credentials.

[What to do if I don't have an IAM account connected to my SDCC credentials](linktbd)

# Submit your first single task 

Adding --submit to the command line will launch the job on PanDA.  You will
also need to specify the username associated with your rucio scope.
```
shrek --user <username> --submit --tag none tests/simple-chain/top.yaml
```

You should see much the same output as before.  There will be a 15 second
pause before the job is submitted.

[What to do if I don't have a rucio scope](linktbd)

At the end you should see output that returns a job ID from PanDA...

```
[shrek INFO] 0 b'(L0L\nVsucceeded. new jediTaskID=13\np0\ntp1\n.'
[shrek INFO] INFO : succeeded. new jediTaskID=13
...
[shrek INFO] Shrek submission none-20230207-2058 2023-02-07 20:58:33.153930 UTC
```

you can now monitor the progress of these jobs by going to ...

https://sphenix-panda.apps.rcf.bnl.gov/

type the jediTaskId into the taskID field in the web form, and hit submit.


# Submit your first workflow

PanDA allows more complicated workflows, chaining jobs together that take
input from others output.  Here we will run such a workflow that has two
jobs named top1 and top2, that feed into a final job named bottom:

```
shrek --user <username> --submit --tag none tests/simple-chain-2to1/top1.yaml \
  tests/simple-chain-2to1/top2.yaml \
  tests/simple-chain-2to1/bottom.yaml 
```
or simply
```
shrek --user <username> --submit --tag none tests/simple-chain-2to1/*.yaml
```
The output will be significantly different than when we submit just a single
job to PanDA... (the panda server returns far more information for single
jobs than for workflows).  

When submitting the workflows, there is a direct link provided to the
task monitoring page.  (Note that the page does not get generated until the
first job in the workflow actually begins running, so the link may be empty
when you first try to follow it).

# Submit something a little more charming...

In the sphenix directory there are two simulation jobs defined, charm and minbias.  
Edit the runCharmSimu.yaml file in the charm directory...
```
$ edt sphenix/charm/runCharmSimu.yaml
```

Change the number of jobs and number of events to what you like, then 

```
shrek --user <username> --submit --tag charm sphenix/charm/runCharmSimu.yaml
```

Enjoy.


