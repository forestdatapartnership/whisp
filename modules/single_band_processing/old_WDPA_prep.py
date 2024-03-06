import ee
import math
from modules.area_stats import get_radius_m_to_buffer_to_required_area

def bufferByArea (feature):
    """Computes the feature's geometry radius from the 'REP_AREA' field and adds it as a property."""
    area = feature.get('REP_AREA')
    
    # buffer_size = get_radius_m_to_buffer_to_required_area(area,"km2")# should work but untested in this function
    
    buffer_size = (ee.Number(feature.get('REP_AREA')).divide(math.pi)).sqrt().multiply(1000) #calculating radius in metres from REP_AREA in km2
    
    return ee.Feature(feature).buffer(buffer_size,1);  ### buffering (incl., max error parameter should be 0m. But put as 1m anyhow - doesn't seem to make too much of a difference for speed)


def filterWDPA (fc): 
    """filtering as per the WDPA Manual: https://www.protectedplanet.net/en/resources/wdpa-manual
    NB could include Biosphere reserves (excluded currently as some buffered by huge areas so not relevant to conservation)""" 
    fcOut = fc.filter(ee.Filter.And(ee.Filter.neq('STATUS', 'Proposed'),ee.Filter.neq('STATUS', 'Not Reported'),ee.Filter.neq('DESIG_ENG', 'UNESCO-MAB Biosphere Reserve')))
    return fcOut