#!/bin/bash

ls | grep part-m | xargs -l sed -i ':a;N;$!ba;s/\n/ /g' 
ls | grep part-m | xargs -l sed -i 's/ \([0-9]\{18\}\)/\n\1/g'