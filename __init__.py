#/roxrec/__init__.py
'''
## ========================================================================== ##
- Roxrec is a package designed to find approximate matches to records
    with non-exact merge fields. 
- MongoDB required to facilitate record distribution and coordination
## Author: Michael Pavlak
## ========================================================================== ##
'''

## Built-in packages
import os
import sys

## Adding additional paths to lib lookup for when unable to install packages to python default libs
PATHS = [os.path.split(__file__)[0],
         os.path.split(os.path.split(__file__)[0])[0],
         'C:/Users/Public/PythonPackages/']

for PATH in PATHS:
    if(PATH not in sys.path):
        sys.path.append(PATH)

## Package
import roxrec



