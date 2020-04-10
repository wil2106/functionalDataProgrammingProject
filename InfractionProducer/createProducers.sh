#!/bin/bash

command="gnome-terminal"

for id in 1 2 3; do
  command+=" --tab -t 'Computing Node $id' -e \"bash -c 'sbt run; read -n1'\""
done
#echo $command
eval $command