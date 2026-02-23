# TEM Lustre Modules setup

This directory contains scripts for setting up modules for TEM (Terrestrial Ecosystem Model).

---

## Module Installation

### 0. Default location (/mnt/exacloud)

```bash
export LUSTRE_HOME=/mnt/exacloud/lustre
export SW=$LUSTRE_HOME/software          # installed packages
export SRC=$LUSTRE_HOME/src              # downloaded tarballs + source
export MODULES=$LUSTRE_HOME/modulefiles  # Lmod modulefiles
chmod +x "$LUSTRE_HOME/install/install_dvmdostem_deps.sh"
```

### 1. Set the dependencies (might take some time)

```bash
bash "$LUSTRE_HOME/install/install_dvmdostem_deps.sh"
```

### 2. Add these lines into slurm job file (modify the corresponding bp template)

```bash
source /etc/profile.d/z00_lmod.sh
module purge
module use /mnt/exacloud/lustre/modulefiles

module load openmpi
module load dvmdostem-deps/2026-02
```

---

## Module Testing

### 3. Test module load interactively

```bash
srun --pty -p dask  -N 1 bash -l
```

Then run:

```bash
source /etc/profile.d/z00_lmod.sh
module purge
module use /mnt/exacloud/lustre/modulefiles
module avail
```

---

## Cluster Setup Instructions

Follow these steps to set up your cluster environment for running TEM:

1. **Pull the latest version** of the repository:
   ```bash
   git clone https://github.com/whrc/Circumpolar_TEM_aux_scripts
   # or, if already cloned:
   cd Circumpolar_TEM_aux_scripts && git pull
   ```

2. **Copy `cluster_install.sh` to your home directory and run it:**
   ```bash
   cp Circumpolar_TEM_aux_scripts/lustre/cluster_install.sh ~/
   bash ~/cluster_install.sh
   ```

3. **Optionally modify the base folder** before running the cluster script:
   ```bash
   # Edit cluster_install.sh and set:
   USER_HOME=${HOME}
   ```

4. **Load modules** — run these lines in your shell or add to your job script:
   ```bash
   source /etc/profile.d/z00_lmod.sh
   module purge
   module use /mnt/exacloud/lustre/modulefiles

   module load openmpi
   module load dvmdostem-deps/2026-026
   ```

5. **Verify modules** — run `module list`; you should see:
   ```
   Currently Loaded Modules:
     1) openmpi/v4.1.x   2) boost/1.80.0   3) hdf5/1.10.9   4) netcdf-c/4.4.1.1   5) dvmdostem-deps/2026-026
   ```

6. **Copy the Makefile** from `Circumpolar_TEM_aux_scripts/lustre/Makefile` to your dvm-dos-tem folder. This Makefile has the correct paths and links to boost and other dependencies (modules).

7. **Compile:**
   ```bash
   make USEMPI=true
   ```

8. **If all steps worked**, you are ready to run the automation script (you may need to edit it before running).

