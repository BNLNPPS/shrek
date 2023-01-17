
cwlVersion: v1.0
class: Workflow
    
requirements:
  MultipleInputFeatureRequirement: {}
inputs: []
# runprocessor : output=runprocessor/outDS
outputs:
  outDS:
    type: string
    outputSource: runprocessor/outDS

steps:
  calorimeters:
    run: prun
    in:
        opt_exec:
          default: "calorimeters.sh  %RNDM:0 >& _calorimeters.log "
        opt_args:
          default: " --nJobs 20  --maxAttempt 1  --outputs emulated-calor.daq  --site BNL_OSG_SPHENIX --avoidVP --noBuild "
    out: [outDS]

  trackers:
    run: prun
    in:
        opt_exec:
          default: "trackers.sh  %RNDM:0 >& _trackers.log "
        opt_args:
          default: " --nJobs 40  --maxAttempt 1  --outputs emulated-tracker.daq  --site BNL_OSG_SPHENIX --avoidVP --noBuild "
    out: [outDS]

  triggers:
    run: prun
    in:
        opt_exec:
          default: "triggers.sh  %RNDM:0 >& _triggers.log "
        opt_args:
          default: " --nJobs 1  --maxAttempt 1  --outputs emulated-trigger.daq  --site BNL_OSG_SPHENIX --avoidVP --noBuild "
    out: [outDS]

  runprocessor:
    run: prun
    in:
        opt_inDS: trackers/outDS
        opt_inDsType:
          default: emulated-tracker.daq
        opt_secondaryDSs: [calorimeters/outDS,triggers/outDS]
        opt_secondaryDsTypes:
          default: [emulated-calor.daq,emulated-trigger.daq]
        opt_exec:
          default: "runprocessor.sh  %RNDM:0 %IN %IN2 %IN3 >& _runprocessor.log "
        opt_args:
          default: " --maxAttempt 1  --outputs manifest.dat  --nFilesPerJob=40 --secondaryDSs IN2:20:%{DS1},IN3:1:%{DS2} --forceStaged  --forceStagedSecondary  --site BNL_OSG_SPHENIX --avoidVP --noBuild "
    out: [outDS]
