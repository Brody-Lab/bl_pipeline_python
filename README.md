# Brody Lab Python-based Data Pipeline

The python data pipeline defined for BrodyLab.

# Installation

## Prerequisites

1. Request access to DataJoint: https://frevvo-prod.princeton.edu/frevvo/web/tn/pu.nplc/u/84fd5e8d-587a-4f6a-a802-0c3d2819e8fe/app/_sO14QHzSEemyQZ_M7RLPOg/formtype/_-XYdEEK2Eeqtf7JjRFmYDQ/popupform
2. Install conda on your system:  https://conda.io/projects/conda/en/latest/user-guide/install/index.html
3. If running in Windows get [git](https://gitforwindows.org/)
4. (Optional for ERDs) [Install graphviz](https://graphviz.org/download/)

## Installation with conda

1. Open a new terminal 
2. Clone this repository: either with https (`https://github.com/Brody-Lab/bl_pipeline_python.git`) or ssh (`git@github.com:Brody-Lab/bl_pipeline_python.git`)
    - If you cannot clone repositories with ssh, [set keys overview](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent), [set keys windows](https://github.com/Brody-Lab/jbreda_labnotebook/blob/master/helpful_code.md#ssh)
3. Create a conda environment: `conda create -n bl_pipeline_python_env python==3.10`
    - the name can be changed, just keep it consistent for your kernal below
    - this repository has been updated to use python 3.10 (3.7 was previously in use) 
4. Activate environment: `conda activate bl_pipeline_python_env`.   **(Activate environment each time you use the project)**
5. Change directory to this repository `cd bl_pipeline_python`.
6. Install the primary required libraries `pip install -e .`
    - this will take a few minutes
7. Install jupyter and ipykernal libraries in series
    ```
    conda install -c conda-forge jupyterlab
    conda install -c anaconda ipykernel
    python -m ipykernel install --user --name=bl_pipeline_python_env # allows you to run notebooks on environment kernal
    ```
8. Run the Configuration notebook under `notebooks/00-datajoint-configuration.ipynb`
    - make sure you select the correct kernal from the top right menu
9. If you have install issues, especially on a windows machine, see [here](https://github.com/Brody-Lab/bl_pipeline_python/blob/main/install_debug.md)
10. Additional libraries you may want to install:
    ```
    pip install seaborn pyarrow openpyxl
    ```

## Tutorials

We have created some tutorial notebooks to help you start working with datajoint

1. Querying data (**Strongly recommended**) 
 - `jupyter notebook notebooks/tutorials/Explore_Sessions_Data.ipynb`
 - `jupyter notebook notebooks/tutorials/1-Explore U19 data pipeline with DataJoint.ipynb`


## Array Electrophysiology with Neuropixels Probes

+ This portion of the workflow usings DataJoint's standard python package 
[element-array-ephys](https://github.com/datajoint/element-array-ephys).

+ This workflow uses the `ephys_chronic` module from `element-array-ephys`.

## Developer Notes

### Database transfer
+ `bl_pipeline_python/datajoint01_pipeline/process/process.py`
+ Copy data from source tables (to shadow tables) to new tables
+ Shadow table allows for renaming of primary key
+ Shadow table has same definition of the new table, except that the primary key
 is the same as the source table
+ For each shadow table set the keys as a secondary field when not used as 
primary key

### Data integrity issues
+ `bl_pipeline_python/notebooks/debugging_data_integrity.ipynb`
