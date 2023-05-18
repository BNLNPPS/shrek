# decode_raw_filename /sphenix/lustre01/sphnxpro/mdc2/rawdata/stripe5/ebdc08_junk-00000329-0010.evt
function decode_raw_filename() {                                                                                                                                                       
  local filename=`basename $1 .evt`                                                                                                                                                    
  local temp=( `echo $filename  | awk -F'[_-]' '{print $1 " " $2 " " $3 " " $4 " "}'` )                                                                                                
  echo ${temp[@]}                                                                                                                                                                      
  # returns stream "junk" run# seq#
}    

# decode_mc_filename /sphenix/lustre01/sphnxpro/mdc2/shijing_hepmc/fm_0_20/g4hits/run0006/G4Hits_sHijing_0_20fm-0000000006-51480.root
function decode_mc_filename() {                                                                                                                                                       
  local filename=`basename $1 .root`                                                                                                                                                    
  local temp=( `echo $filename  | awk -F'[_-]' '{print $1 " " $2 " " $5 " " $6 " " $3 " " $4 }'` )                                                                                                  
  echo ${temp[@]}                                                                                                                                                                      
  # returns "G4Hits" gener run# seq# b_min b_mx 
}    

function decode_dst_vtx_filename() {
# decode_dst_vtx_filename /sphenix/lustre01/sphnxpro/mdc2/shijing_hepmc/fm_0_20/pileup/run0006/DST_VERTEX_sHijing_0_20fm_50kHz_bkg_0_20fm-0000000006-00000.root
#                                                                       1     2      3    4  5     6    7  8  9      10       11
  local filename=`basename $1 .root` 
  local temp=( `echo $filename  | awk -F'[_-]' '{print $1 "_" $2 " " $3 " " $4 "_" $5 " " $6 "_" $7 " " $8 "_" $9 " " $10 " " $11 }'` )
  echo ${temp[@]}
}

function decode_dst_calo_filename() {
# /sphenix/lustre01/sphnxpro/mdc2/shijing_hepmc/fm_0_20/pileup/run0006/DST_CALO_G4HIT_sHijing_0_20fm_50kHz_bkg_0_20fm-0000000006-00000.root
#                                                                       1    2    3     4     5  6    7     8  9  10      11       12
  local filename=`basename $1 .root` 
  local temp=( `echo $filename  | awk -F'[_-]' '{print $1 "_" $2 "_" $3 " " $4 " " $5 "_" $6 " " $7 "_" $8 " " $9 "_" $10 " " $11 " " $12 }'` )
  echo ${temp[@]}
}

function decode_dst_bbc_filename() {
# decode_dst_bbc_filename /sphenix/lustre01/sphnxpro/mdc2/shijing_hepmc/fm_0_20/pileup/run0006/DST_BBC_G4HIT_sHijing_0_20fm_50kHz_bkg_0_20fm-0000000006-00007.root    
#                                                                       1    2    3     4     5  6    7     8  9  10      11       12
  local filename=`basename $1 .root` 
  local temp=( `echo $filename  | awk -F'[_-]' '{print $1 "_" $2 "_" $3 " " $4 " " $5 "_" $6 " " $7 "_" $8 " " $9 "_" $10 " " $11 " " $12 }'` )
  echo ${temp[@]}
}

function sphenix_init() {
    echo [ Initializing sPHENIX environment $@ ]
    source /opt/sphenix/core/bin/sphenix_setup.sh $@
}

function check_runsequence() {
    if [[ $1 != $2 ]]
    then
       echo $5
    fi
    if [[ $3 != $4 ]]
    then 
       echo $5
    fi
}

