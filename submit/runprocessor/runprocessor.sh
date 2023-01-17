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
export IN1_name=trackers/outDS
export IN1_task=trackers
export IN1=(`echo $2 | tr "," " "`)
export IN2_name=calorimeters/outDS
export IN2_task=calorimeters
export IN2=(`echo $3 | tr "," " "`)
export IN3_name=triggers/outDS
export IN3_task=triggers
export IN3=(`echo $4 | tr "," " "`)
export shrek_tag=runprocessor
export name=runprocessor
export comment=Run processor emulation



echo ---------------------------------------------------------
echo bottom.sh:
cat bottom.sh
echo ---------------------------------------------------------

echo "Emulated tracker inputs"
echo ${IN1[@]}
echo "Emulated calorimeter inputs"
echo ${IN2[@]}
echo "Emulated trigger inputs"
echo ${IN3[@]}

echo "Emulated tracker inputs"     >  manifest.dat
echo ${IN1[@]}                     >> manifest.dat
echo "Emulated calorimeter inputs" >> manifest.dat
echo ${IN2[@]}                     >> manifest.dat
echo "Emulated trigger inputs"     >> manifest.dat
echo ${IN3[@]}                     >> manifest.dat

cp _${name}.log /sphenix/u/sphnxpro/shrek/_${name}-${uniqueId}.log
