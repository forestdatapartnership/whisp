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

def image_coll_binary_to_area_w_properties_w_exceptions(image_collection,exception_dataset_id,debug=False):
    
    images_to_convert = image_collection.filter(ee.Filter.neq("dataset_id",exception_dataset_id))

    image_staying_binary = image_collection.filter(ee.Filter.eq("dataset_id",exception_dataset_id))

    images_w_area = image_coll_binary_to_area_w_properties(images_to_convert)

    combined_image_collection = images_w_area.merge(image_staying_binary)

    if debug: print ("dataset_ids in final image_collection:", combined_image_collection.aggregate_array("dataset_id").getInfo())
    
    return combined_image_collection


def image_coll_binary_to_area_w_properties(image_collection):
    image_collection_w_area = image_collection.map(area_stats.binary_to_area_w_properties)
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


def find_country_from_modal_stats(roi,image_collection,reducer_choice,geo_id_column,dataset_name,admin_code_col_name,lookup_country_codes_to_names):
    
    zonal_stats_country_codes = zonal_stats_iCol(roi,
                                      image_collection.filter(ee.Filter.eq("country_allocation_stats_only",1)),
                                      reducer_choice)# all but alerts
    
    df_stats_country_codes = geemap.ee_to_pandas(zonal_stats_country_codes) #NB limit of 5000 (unlikely to need more for demo but i have code for it if this happens)
    
    #get mode stats for dataset
    lookup_geo_id_to_country_codes = df_stats_country_codes[df_stats_country_codes["dataset_name"]==dataset_name] 
    
    #choose only columns needed
    lookup_geo_id_to_country_codes = lookup_geo_id_to_country_codes[[geo_id_column, 'mode']]
    
    # change names for a clean join 
    lookup_geo_id_to_country_codes = lookup_geo_id_to_country_codes.rename(columns={"mode":admin_code_col_name})
    
    lookup_geo_id_to_country_names = lookup_geo_id_to_country_codes.merge(lookup_country_codes_to_names,on=admin_code_col_name,how="inner").drop(admin_code_col_name,axis=1) # join geo id to the lookup_table countaining "Country_names"
    # lookup_geo_id_to_ISO3 = lookup_geo_id_to_GAUL_codes.merge(lookup_country_codes_to_ISO3,on="ADM0_CODE",how="inner").drop("ADM0_CODE",axis=1) # join geo id to the GAUL_lookup_table countaining "Country_names"

    return lookup_geo_id_to_country_names
 

