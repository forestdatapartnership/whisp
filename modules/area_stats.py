import ee
import geemap
# import area_stats

def zonal_stats_plot_w_buffer (roi, roi_buffer, images_iCol, plot_stats_list, buffer_stats_list, reducer_choice):
    """combines zonal_stats_iCol for plot with (alert) stats for buffer zone around it"""                       
    ## get stats for roi (not including deforestation alerts)
    zonal_stats_plot = zonal_stats_iCol(roi,images_iCol.filter(ee.Filter.inList("system:index",plot_stats_list)),
                                                                      reducer_choice)# all but alerts
    ## get stats for buffer (alerts only)
    zonal_stats_buffer = zonal_stats_iCol(roi_buffer,images_iCol.filter(ee.Filter.inList(
                                                        "system:index",buffer_stats_list)),
                                                        reducer_choice) #alerts only
    
    #combine stats from roi and buffer into one feature collection
    zonal_stats_plot_w_buffer = zonal_stats_plot.merge(zonal_stats_buffer) 
    return zonal_stats_plot_w_buffer


def zonal_stats_iCol (featureCollection,imageCollection,reducer_choice):
    "Calculating summary statistics for each image in collection, within each feature in a collection""" 
  
    def zonal_stats (image):
        scale=image.get("scale")
        fc = ee.FeatureCollection(image.reduceRegions(collection=featureCollection,
                                                      reducer=reducer_choice,
                                                      scale=scale))
        fc = fc.map(lambda feature: feature.set("dataset_name",image.get("system:index")))
        return fc
    
    fc_out = imageCollection.map(zonal_stats).flatten()
    
    return fc_out


def set_scale_property_from_image(target_image,template_image,template_band_index=0,debug=False):
    """Gets nominal scale from template image and sets it as "scale" property in target image. 
Template images are used for when target image scale property has been lost (e.g. after using '.mosaic()') """
    out_scale = get_scale_from_image(template_image,band_index=template_band_index)
    if debug:
        # print("template_band_index: ",template_band_index)
        print("scale (m): ",out_scale)
    output_image = target_image.set("scale",out_scale)
    return output_image




def get_scale_from_image(image,band_index=0):
    """gets nominal scale from image (NB this should not be from a composite/mosaic or incorrrect value returned)"""
    return image.select(band_index).projection().nominalScale().getInfo()



def image_coll_binary_to_area_w_properties_w_exceptions(image_collection,exception_dataset_id_list,debug=False):
    
    images_to_convert = image_collection.filter(
        ee.Filter(ee.Filter.inList("dataset_id",exception_dataset_id_list)).Not())
    
    images_staying_binary = image_collection.filter(
        ee.Filter.inList("dataset_id",exception_dataset_id_list))

    images_w_area = image_coll_binary_to_area_w_properties(images_to_convert)

    combined_image_collection = images_w_area.merge(images_staying_binary)

    if debug: print ("dataset_ids in final image_collection:", combined_image_collection.aggregate_array("dataset_id").getInfo())
    
    return combined_image_collection


def image_coll_binary_to_area_w_properties(image_collection):
    image_collection_w_area = image_collection.map(binary_to_area_w_properties)
    return image_collection_w_area
    


def binary_to_area_w_properties(image,to_pixel_area=True,copy_properties=True,debug=True):
    """get pixel area in hectares for image and copyn properties to new image (defaultes to true)"""
    image_area = binary_to_area_hectares(image,to_pixel_area)
    if copy_properties:
        out_image = image_area.copyProperties(image)
        if debug: print ("copying properties") 
    else:
        out_image = image_area
        if debug: print ("not copying properties") 
    return out_image



def binary_to_area_hectares(image,to_pixel_area=True):
    """get pixel area in hectares for image"""
    if to_pixel_area:
        out_image = image.multiply(ee.Image.pixelArea()).divide(1e4) 
    else:
        out_image = image
    return out_image




def add_area_hectares_property_to_feature_collection(fc,geometry_area_column):
    def add_area_hectares_property_to_feature (feature):
        feature = feature.set(geometry_area_column,feature.area().divide(1e4))#add area
        return feature
    outFC = fc.map(add_area_hectares_property_to_feature)
    return outFC



def buffer_point_to_required_area(feature,area,area_unit):
    """buffers feature to get a given area (needs math library); area unit in 'ha' or 'km2' (the default)"""
    area = feature.get('REP_AREA')
    
    # buffer_size = get_radius_m_to_buffer_for_given_area(area,"km2")# should work but untested in this function
    
    buffer_size = (ee.Number(feature.get('REP_AREA')).divide(math.pi)).sqrt().multiply(1000) #calculating radius in metres from REP_AREA in km2
    
    return ee.Feature(feature).buffer(buffer_size,1);  ### buffering (incl., max error parameter should be 0m. But put as 1m anyhow - doesn't seem to make too much of a difference for speed)


def get_radius_m_to_buffer_to_required_area(area,area_unit="km2"):
    """gets radius in metres to buffer to get an area (needs math library); area unit ha or km2 (the default)"""
    if area_unit=="km2": unit_fix_factor =1000
    elif area_unit=="ha": unit_fix_factor =100
    radius = ee.Number(area).divide(math.pi).sqrt().multiply(unit_fix_factor)
    return radius


 

