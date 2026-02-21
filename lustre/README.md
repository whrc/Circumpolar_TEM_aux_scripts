# TEM Lustre Modules setup

This directory contains scripts for setting up modules for TEM (Terrestrial Ecosystem Model).

## 0.Default location (/mnt/exacloud)

```bash
export LUSTRE_HOME=/mnt/exacloud/lustre
export SW=$LUSTRE_HOME/software          # installed packages
export SRC=$LUSTRE_HOME/src              # downloaded tarballs + source
export MODULES=$LUSTRE_HOME/modulefiles  # Lmod modulefiles
chmod +x "$LUSTRE_HOME/install/install_dvmdostem_deps.sh"
```

## 1.Set the dependancies ( might take some time).
```bash
bash "$LUSTRE_HOME/install/install_dvmdostem_deps.sh"
```

## 2.Add these lines into slurm job file (modify the correspoding bp template).
```bash
source /etc/profile.d/z00_lmod.sh
module purge
module use /mnt/exacloud/lustre/modulefiles

module load openmpi
module load dvmdostem-deps/2026-02
```

## 3.Copy Makefile to dvm-dos-tem folder
Before compiling make sure to load the modules as shown in step 2. 
```bash
make USEMPI=true
```

## 4.To test module load interactively 
```bash
srun --pty -p dask  -N 1 bash -l
```
then run
```bash
source /etc/profile.d/z00_lmod.sh
module purge
module use /mnt/exacloud/lustre/modulefiles
module avail
```

