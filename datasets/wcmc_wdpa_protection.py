import os
import ee

# dataset_id = 13

def wcmc_wdpa_protection_prep(dataset_id, template_image):
    
    import modules.WDPA_prep as WDPA_prep
    import modules.area_stats as area_stats
    from datasets.reproject_to_template import reproject_to_template
    
    # wdpa_pnt = ee.FeatureCollection("WCMC/WDPA/current/points");

    wdpa_poly = ee.FeatureCollection("WCMC/WDPA/current/polygons");

    #apply filters and merge polygon with buffered points  
    wdpa_filt = WDPA_prep.filterWDPA(wdpa_poly) ##.merge(WDPA_prep.filterWDPA(wdpa_pnt).filter(ee.Filter.gt('REP_AREA', 0)).map(WDPA_prep.bufferByArea));
    #turn into image (no crs etc set currently)
    wdpa_overlap = wdpa_filt.reduceToImage(['STATUS_YR'],'min');  #make into raster - remove mask if want 0s

    #make binary
    wdpa_binary = wdpa_overlap.lt(2070).unmask()

    #reproject based on template (tyically gfc data - approx 30m res)
    wdpa_binary_reproj = reproject_to_template(wdpa_binary,template_image)

    wdpa_binary_reproj = area_stats.set_scale_property_from_image(
        wdpa_binary_reproj,template_image,0,debug=True)

    output_image = wdpa_binary_reproj
    
    return output_image.set("dataset_id",dataset_id)