import ee
import pandas as pd
import geopandas as gpd
import json
import geemap

import requests # may not be needed
from sidecar import Sidecar

from modules.gee_initialize import initialize_ee 
initialize_ee()

import pickleshare 

def whisp_alt_stats_as_df (roi): 
    %store roi
    %run alt_whisp_stats.ipynb   
    return df_out

def whisp_stats_as_df (roi): 
    %store roi
    %run whisp_stats.ipynb   
    return df_out