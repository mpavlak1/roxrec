#/roxrec/general.py
'''
## ========================================================================== ##
- Generic helper functions used accross multiple classes
* Temporary place to dump helper functions, potentially will move later
## Author: Michael Pavlak
## ========================================================================== ##
'''

## Built-ins
import hashlib
from math import sqrt

## Package
import __init__

## Additional Packages
from nltk import edit_distance
from xxhash import xxh32 as h32

## List index functions
def first(L): return L[0]
def second(L): return L[1]
def last(L): return L[-1]

## String Similarity Functions
def string_sim(s0,s1):
    return 1 - (edit_distance(s0,s1) / max(len(s0),len(s1)))

def cosine_sim(v0, v1):
    L = len(v0)
    assert len(v0) == L and len(v1) == L

    (v0v1, v0v0, v1v1) = (0,0,0)
    for i in range(L):
        v0v1 += v0[i]*v1[i]
        v0v0 += v0[i]*v0[i]
        v1v1 += v1[i]*v1[i]
    try:
        return v0v1/(sqrt(v0v0)*sqrt(v1v1))
    except ZeroDivisionError:
        return 0

## Hashing functions
def hashstring(s):
    return str(int.from_bytes(h32(s).digest(), byteorder='little'))

def sha1_file(filepath):
    with open(filepath,mode='r',encoding='UTF-8',errors='ignore') as r:
        return hashlib.sha1(r.read().encode()).hexdigest()



