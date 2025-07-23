# make sure condarc is avaailible
touch ~/.condarc
vim ~/.condarc

## --- paste the contents below to the file --- ##
pkgs_dirs:
  - /projects/$USER/.conda_pkgs
envs_dirs:
  - /projects/$USER/software/anaconda/envs


# instructions to create new conda enviornment
module load anaconda
conda create --name neuroclass2025 python=3.9 
conda activate neuroclass2025
pip install -r requirements.txt

# last conda setup... add ipykernel so you can use jupyter
python -m ipykernel install --user --name flywheel --display-name flywheel


# make a local copy of the flywheel command line interface
cp /scratch/alpine/amhe4269/fw /projects/$USER/software/anaconda/envs/neuroclass2025/bin/fw


