#!/bin/bash

#SBATCH --job-name=test
#SBATCH --ntasks-per-node=1
#SBATCH --nodes=1
#SBATCH --time=05:00
#SBATCH --partition=dp-cn


/p/home/jusers/faltynek1/deep/sbb/hello.sh