# Deker
![image](https://upload.wikimedia.org/wikipedia/commons/thumb/d/d2/10-cube.svg/480px-10-cube.svg.png)

Multidimensional spatial raster storage
## Requirements
- python >= 3.10
- h5py
- hdf5plugin

### ARM

If you want to run Deker library on your Mac, 
you need x86_64 python installed with Rosetta - Deker uses NumPy,
and some NumPy types are unsupported on current NumPy ARM version.  

Follow this [guide](https://towardsdatascience.com/how-to-use-manage-multiple-python-versions-on-an-apple-silicon-m1-mac-d69ee6ed0250) or next steps:
1. Install Rosetta (ARM -> x86_64 translator): `softwareupdate --install-rosetta`
2. Create a Rosetta terminal: 
   - Duplicate your terminal (apps -> utilities -> right click) or install new.   
   - Click "Get info" on new terminal and set "Open using Rosetta"  
3. Install homebrew: `/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"`  
4. Add alias to your zsh config file: `alias rbrew="arch -x86_64 /usr/local/bin/brew"`  
5. Install python: `rbrew install python@3.10`  

After that you can install Deker   
