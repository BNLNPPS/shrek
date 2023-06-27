#!/usr/bin/bash -f

#   00009451
nevents=100
run="00009245"
dir=/sphenix/lustre01/sphnxpro/commissioning/emcal/beam
submitopt=" --submit "
debugopt=" "
if [[ $1 ]]; then
   run=$( printf "%08d" $1 )
fi
if [[ $2 ]]; then
   dir=$2
fi

scope=user.jwebb2
tag=sP22x


# Clean out / create temp directory for filelists
if [ -e /tmp/${USER}/$run ]; then
   rm -r /tmp/${USER}/$run
fi
mkdir /tmp/${USER}/$run -p

find ${dir} -type f -name *seb00*${run}-????.prdf -print | sort > /tmp/${USER}/$run/seb00.list
find ${dir} -type f -name *seb01*${run}-????.prdf -print | sort > /tmp/${USER}/$run/seb01.list
find ${dir} -type f -name *seb02*${run}-????.prdf -print | sort > /tmp/${USER}/$run/seb02.list
find ${dir} -type f -name *seb03*${run}-????.prdf -print | sort > /tmp/${USER}/$run/seb03.list
find ${dir} -type f -name *seb04*${run}-????.prdf -print | sort > /tmp/${USER}/$run/seb04.list
find ${dir} -type f -name *seb05*${run}-????.prdf -print | sort > /tmp/${USER}/$run/seb05.list
find ${dir} -type f -name *seb06*${run}-????.prdf -print | sort > /tmp/${USER}/$run/seb06.list
find ${dir} -type f -name *seb07*${run}-????.prdf -print | sort > /tmp/${USER}/$run/seb07.list

ls /tmp/${USER}/$run/seb00.list  > run-${run}.filelist
ls /tmp/${USER}/$run/seb01.list >> run-${run}.filelist
ls /tmp/${USER}/$run/seb02.list >> run-${run}.filelist
ls /tmp/${USER}/$run/seb03.list >> run-${run}.filelist
ls /tmp/${USER}/$run/seb04.list >> run-${run}.filelist
ls /tmp/${USER}/$run/seb05.list >> run-${run}.filelist
ls /tmp/${USER}/$run/seb06.list >> run-${run}.filelist
ls /tmp/${USER}/$run/seb07.list >> run-${run}.filelist

tagin=`tail -n 1 filetag`
echo FILETAG IS ${tagin}
echo $(( tagin + 1 )) >> filetag

shrek ${submitopt} ${debugopt} --outDS ${scope}.${tag}_test${tagin}_calor_calib --nevents=${nevents} --no-pause --tag calor-calib workflows/rhic2023.AuAu200.calor/*.yaml --filetag=test${tagin} --runNumber=${run} --filelist=run-${run}.filelist 


