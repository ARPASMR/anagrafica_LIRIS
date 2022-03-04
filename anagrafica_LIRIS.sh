#!/bin/bash
#=============================================================================
# Lo script è all'interno del container e lancia uno script R ogni 1h. 
# Lo script R estrae le info di anagrafica per LIRIS dal DBmeteo e le importa 
# nella tabella anagraficasensori del DB postgres di LIRIS. 
#
# 2022/03/04 AV + MR
#=============================================================================
numsec=3600 # 1 ore 
/usr/bin/Rscript anagrafica_LIRIS.R
sleep $numsec
while [ 1 ]
do
  if [ $SECONDS -ge $numsec ]
  then
    /usr/bin/Rscript anagrafica_LIRIS.R
    SECONDS=0
    sleep $numsec
  fi
done
