#!/bin/bash
source ~/projects/ebd_env/bin/activate
cd ~/projects
jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --notebook-dir=~/projects/notebooks
