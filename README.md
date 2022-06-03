# efficient_selenium
Try to speed up webcrawling with the combination of Ray and selenium

# Install on Colab VSCode:

. instal.sh 

# Install on M1 MAC
Ref: https://docs.ray.io/en/latest/ray-overview/installation.html#m1-mac-apple-silicon-support

1. download .sh from https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-MacOSX-arm64.sh
2. run `bash Miniforge3-MacOSX-arm64.sh`
3. `source miniforge3/bin/activate`
4. `conda activate`
5. `pip uninstall grpcio; conda install grpcio==1.43.0`
6. `pip install ray`
7. `pip install -r requirements.txt`
