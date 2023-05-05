Bash scripts in this directory will be copied into the working directory
of the worker node and sourced at the start of the user shell script,
following the creation of the shrek environment.  This allows the user to
defined bash functions which can be used w/in the shell environment.