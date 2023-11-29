import ee

def reduceStatsIC (featureCollection,imageCollection,reducer_choice):
    "Calculating summary statistics for each image in collection, within each feature in a collection""" 
  
    def reduceStats (image):
        scale=image.get("scale")
        fc = ee.FeatureCollection(image.reduceRegions(collection=featureCollection,
                                                      reducer=reducer_choice,
                                                      scale=scale))
        fc = fc.map(lambda feature: feature.set("dataset_name",image.get("system:index")))
        return fc
    
    fc_out = imageCollection.map(reduceStats).flatten()
    
    return fc_out



def set_scale_property_from_image(target_image,template_image,template_band_index=0,verbose=False):
    """Gets nominal scale from template image and sets it as "scale" property in target image. 
Template images are used for when target image scale property has been lost (e.g. after using '.mosaic()') """
    out_scale = get_scale_from_image(template_image,band_index=template_band_index)
    if verbose:
        print("template_band_index: ",template_band_index)
        print("scale (m): ",out_scale)
    output_image = target_image.set("scale",out_scale)
    return output_image


def get_scale_from_image(image,band_index=0):
    """gets nominal scale from image (NB this should not be from a composite/mosaic or incorrrect value returned)"""
    return image.select(band_index).projection().nominalScale().getInfo()

    
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

# # add area property in hectares for input feature (ROI)
# def add_area_hectares_property_to_feature (feature):
#     feature = feature.set(geometry_area_column,feature.area().divide(1e6))#add area
#     # if verbose:
#     #     print(feature.get(geometry_area_column).getInfo())
#     return feature