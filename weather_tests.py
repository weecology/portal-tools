"""Tests Weather_python_sql.py code which is intended to enter and append data to the database"""
import nose
from Weather_python_sql.py import *

dataline = [101.0, 2010.0, 44.0, 800.0, 0.0, 2.13, 68.25, None, 2, 13]

def test_jday2caldates_min():
    assert jday2caldates([101.0, 2010.0, 1.0, 800.0, 0.0, 2.13, 68.25, None, 2, 13]) == [101.0, 2010.0, 1.0, 800.0, 0.0, 2.13, 68.25, None, 2, 13, 1, 1]
    
def test_jday2caldates_max():
    assert jday2caldates([101.0, 2010.0, 365.0, 800.0, 0.0, 2.13, 68.25, None, 2, 13]) == [101.0, 2010.0, 365.0, 800.0, 0.0, 2.13, 68.25, None, 2, 13, 12, 31]
    