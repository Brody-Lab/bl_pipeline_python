# Brody Lab Python-based Data Pipeline

The python data pipeline defined for BrodyLab.

# Installation

## Prerequisites

1. Install conda on your system:  https://conda.io/projects/conda/en/latest/user-guide/install/index.html
2. If running in Windows get [git](https://gitforwindows.org/)
3. (Optional for ERDs) [Install graphviz](https://graphviz.org/download/)

## Installation with conda

1. Open a new terminal 
2. Clone this repository: `git@github.com:Brody-Lab/bl_pipeline_python.git`
    - If you cannot clone repositories with ssh, [set keys](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent)
3. Create a conda environment: `conda create -n bl_pipeline_python_env python==3.7`.
4. Activate environment: `conda activate bl_pipeline_python_env`.   **(Activate environment each time you use the project)**
5. Change directory to this repository `cd bl_pipeline_python`.
6. Install all required libraries `pip install -e .`
7. Datajoint Configuration: `jupyter notebook notebooks/00-datajoint-configuration.ipynb` 

## Tutorials

We have created some tutorial notebooks to help you start working with datajoint

1. Querying data (**Strongly recommended**) 
 - `jupyter notebook notebooks/tutorials/Explore_Sessions_Data.ipynb`
 - `jupyter notebook notebooks/tutorials/1-Explore U19 data pipeline with DataJoint.ipynb`


## Array Electrophysiology with Neuropixels Probes

+ This portion of the workflow usings DataJoint's standard python package 
[element-array-ephys](https://github.com/datajoint/element-array-ephys).

+ This workflow uses the `ephys_chronic` module from `element-array-ephys`.

## User instructions
### Set up a working environment

+ It is highly recommended to create a virtual environment either with 
virtualenv or conda.
  + Create conda environment
    ```
    conda create -n bl_env python==3.8
    ```
  + After the environment is created
    ```
    conda activate bl_env
    ```

+ To keep your own version of the pipeline and contribute, fork this repository.

+ Clone your fork
  ```
  git clone {your_fork_url}.git
  ```

+ Install this repository by changing into the directory of this repository and
 run `pip install -e .`.  With the `-e` flag, your modifications on the pipeline 
 will be reflected immediately on the imported modules.

+ Configure the `dj_local_conf.json`
  + In the root of the cloned repository, create a new file `dj_local_conf.json` 
  with the following template:

  ```json
  {
    "database.host": "<hostname>",
    "database.user": "<username>",
    "database.password": "<password>",
    "loglevel": "INFO",
    "safemode": true,
    "display.limit": 7,
    "display.width": 14,
    "display.show_tuple_count": true,
    "custom": {
        "database.prefix": "bl_new_",
        "ephys_root_data_dir": ["<archive drive>", "<cup drive>"]
      }
  }
  ```

  + For the ephys workflow, define the `ephys_root_data_dir` as a list of 
  directories with the raw and processed data.
    + The directory paths will be specific to your machine.
    + Raw Neuropixels data
      + Located on archive drive
      + To mount cup on linux
        ```
        sudo mount -t cifs //apps02.pni.princeton.edu/archive-labdata /home/pu.win.princeton.edu/<Princeton netID>/mnt_archive -o username=<Princeton netID>,domain=PRINCETON,iocharset=utf8,rw,file_mode=0664,dir_mode=0775,nolinux,noperm,vers=2.1
        ```
      + Mount point on linux
        ```
        /home/pu.win.princeton.edu/<Princeton netID>/mnt_archive/brody/RATTER/PhysData/Raw/
        ```

    + Processed Kilosort data
      + Located on cup drive
      + To mount cup on linux
        ```
        sudo mount -t cifs //cup.pni.princeton.edu/brody /home/pu.win.princeton.edu/<Princeton netID>/mnt_cup -o username=<Princeton netID>,domain=PRINCETON,iocharset=utf8,rw,file_mode=0664,dir_mode=0775,nolinux,noperm,vers=2.1
        ```
      + Mount point on linux
        ```
        /home/pu.win.princeton.edu/<Princeton netID>/mnt_cup/labs/brody/RATTER/PhysData/NP_sorted/
        ```

+ Start a python kernel in the root of the cloned repository directory and 
DataJoint will be configured properly.

## Developer notes

+ Create tables with new definitions and copy to new server.

### Database transfer
+ `bl_pipeline_python/process/process.py`
+ Copy data from source tables (to shadow tables) to new tables
+ Shadow table allows for renaming of primary key
+ Shadow table has same definition of the new table, except that the primary key
 is the same as the source table
+ For each shadow table set the keys as a secondary field when not used as 
primary key

### Data integrity issues
+ `bl_pipeline_python/notebooks/debugging_data_integrity.ipynb`
