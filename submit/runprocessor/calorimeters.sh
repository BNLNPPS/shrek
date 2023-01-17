#!/usr/bin/env bash

echo $@

echo Executing on `hostname`
uname -a
lscpu | grep \^CPU
free -h --giga

export shrek_username=jwebb2
export shrek_tag=runprocessor-20230117-1631
export rucio_dsname=user.jwebb2.runprocessor-20230117-1631
export uniqueId=$1
export shrek_tag=runprocessor
export name=calorimeters
export comment=Emulation of the sPHENIX calorimeters
export nJobs=20


echo $uniqueId > emulated-calor.daq
