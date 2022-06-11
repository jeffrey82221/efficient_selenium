# efficient_selenium
Try to speed up webcrawling with the combination of Ray and selenium

# Install on Colab VSCode:

. instal.sh 

# Install RAY on M1 MAC
Ref: https://docs.ray.io/en/latest/ray-overview/installation.html#m1-mac-apple-silicon-support

1. download .sh from https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-MacOSX-arm64.sh
2. run `bash Miniforge3-MacOSX-arm64.sh`
3. `source miniforge3/bin/activate`
4. `conda activate`
5. `pip uninstall grpcio; conda install grpcio==1.43.0`
6. `pip install ray`
7. `pip install -r requirements.txt`

# Install PyTables on M1 MAC


1. `brew install hdf5 c-blosc lzo bzip2`
2. `conda install pytables`



TODO: 
- [ ] Scan and create a download report for every fund with info of company , isin , name , last_nav_date, last_update_datee
- [X] Fix selenium time zone 
