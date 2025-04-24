#!/bin/bash
#SBATCH --export=all
#SBATCH --ntasks=8                  # total parallel tasks
#SBATCH --cpus-per-task=8
#SBATCH --mem=2G

# Collect and sort all .tar files
mapfile -t tar_files < <(printf '%s\n' "$DATA_PATH"/*.tar | sort)
num_files=${#tar_files[@]}
num_tasks=$SLURM_NTASKS
task_id=$SLURM_PROCID # 0-based rank of this task

# Single loop: pick every Nth file for this task and build "-f" args
args=()
for ((i = task_id; i < num_files; i += num_tasks)); do
  args+=(-f "${tar_files[i]}")
done

# Run the program on this slice of files
srun ./indexer "${args[@]}"
