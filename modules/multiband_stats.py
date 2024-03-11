import ee
# ee.Initialize()
# from modules.gee_initialize import initialize_ee 
# initialize_ee()
# ee.Initialize(project="ee-andyarnellgee")
from datetime import datetime

import functools

def get_scale_from_image(image,band_index=0):
    """gets nominal scale from image (NB this should not be from a composite/mosaic or incorrrect value returned)"""
    return image.select(band_index).projection().nominalScale().getInfo()


def reproject_to_template(rasterised_vector,template_image):
    """takes an image that has been rasterised but without a scale (resolution) and reprojects to template image CRS and resolution"""
    #reproject an image
    crs_template = template_image.select(0).projection().crs().getInfo()
    
    output_image = rasterised_vector.reproject(
      crs= crs_template,
      scale= get_scale_from_image(template_image),
    ).int8()
    
    return output_image

# Function to reorder properties
def reorder_properties(feature, order):
    properties = {key: feature.get(key) for key in order}
    return ee.Feature(feature.geometry(), properties) 

# Function to add ID to features
def add_id_to_feature(feature, join):
    index = feature.get('system:index')
    return feature.set(id_name, index)

# Function to flag positive values
def flag_positive_values(feature,flag_positive):
    for prop_name in flag_positive:
        flag_value = ee.Algorithms.If(ee.Number(feature.get(prop_name)).gt(0), 'True', '-')
        feature = feature.set(prop_name, flag_value)
    return feature

# Function to round properties to whole numbers
def round_properties_to_whole_numbers(feature,round_properties):
    for prop_name in round_properties:
        prop_value = feature.get(prop_name)
        prop_value_rounded = ee.Number(prop_value).round()
        feature = feature.set(prop_name, prop_value_rounded)
    return feature

# Function to exclude properties
def copy_properties_and_exclude(feature,exclude_properties):
    return ee.Feature(feature.geometry()).copyProperties(source=feature, exclude=exclude_properties)

# # Function to select and rename properties
# def select_and_rename_properties(feature):
#     first_feature = ee.Feature(feature_collection.first())
#     property_names = first_feature.propertyNames().getInfo()
#     new_property_names = [prop.replace('_', ' ') for prop in property_names]
#     return feature.select(property_names, new_property_names)

######################################


def creaf_descals_palm_prep():
    oil_palm_descals_raw = ee.ImageCollection('BIOPAMA/GlobalOilPalm/v1')
    oil_palm_descals_mosaic = oil_palm_descals_raw.select('classification').mosaic()
    return oil_palm_descals_mosaic.lte(2).rename("Oil_palm_Descals")

def jaxa_forest_prep():
    jaxa_forest_non_forest_raw = ee.ImageCollection('JAXA/ALOS/PALSAR/YEARLY/FNF4')
    jaxa_forest_non_forest_2020 = jaxa_forest_non_forest_raw.filterDate('2020-01-01', '2020-12-31').select('fnf').mosaic()
    return jaxa_forest_non_forest_2020.lte(2).rename("JAXA_FNF_2020")

def esri_lulc_trees_prep():
    esri_lulc10 = ee.ImageCollection("projects/sat-io/open-datasets/landcover/ESRI_Global-LULC_10m_TS")
    esri_lulc10_2020 = esri_lulc10.filterDate('2020-01-01', '2020-12-31').map(lambda image: image.remap([1, 2, 4, 5, 7, 8, 9, 10, 11], [1, 2, 3, 4, 5, 6, 7, 8, 9])).mosaic()
    return esri_lulc10_2020.eq(2).rename("ESRI_TC_2020")	

def glad_gfc_10pc_prep():
    gfc = ee.Image("UMD/hansen/global_forest_change_2022_v1_10")
    gfc_treecover2000 = gfc.select(['treecover2000'])
    gfc_loss2001_2020 = gfc.select(['lossyear']).lte(20)
    gfc_treecover2020 = gfc_treecover2000.where(gfc_loss2001_2020.eq(1), 0)
    return gfc_treecover2020.gt(10).rename("GFC_TC_2020")

def glad_lulc_stable_prep():
    glad_landcover2020 = ee.Image('projects/glad/GLCLU2020/v2/LCLUC_2020').updateMask(ee.Image("projects/glad/OceanMask").lte(1))
    glad_landcover2020_main = glad_landcover2020.where(glad_landcover2020.gte(27).And(glad_landcover2020.lte(48)), 27).where(glad_landcover2020.gte(127).And(glad_landcover2020.lte(148)), 27)
    return glad_landcover2020_main.eq(27).rename("GLAD_LULC_2020")

def glad_pht_prep():
    primary_ht_forests2001_raw = ee.ImageCollection('UMD/GLAD/PRIMARY_HUMID_TROPICAL_FORESTS/v1')
    primary_ht_forests2001 = primary_ht_forests2001_raw.select("Primary_HT_forests").mosaic().selfMask()
    gfc = ee.Image('UMD/hansen/global_forest_change_2022_v1_10')
    gfc_loss2001_2020 = gfc.select(['lossyear']).lte(20)
    return primary_ht_forests2001.where(gfc_loss2001_2020.eq(1), 0).rename("GLAD_Primary")

def jrc_gfc_2020_prep():
    jrc_gfc2020_raw = ee.ImageCollection("JRC/GFC2020/V1")
    return jrc_gfc2020_raw.mosaic().rename("EUFO_2020")

def jrc_tmf_disturbed_prep():
    jrc_tmf_transitions_raw = ee.ImageCollection('projects/JRC/TMF/v1_2020/TransitionMap_Subtypes')
    jrc_tmf_transitions = jrc_tmf_transitions_raw.mosaic()
    in_list = [21, 22, 23, 24, 25, 26, 61, 62, 31, 32, 33, 63, 64, 51, 52, 53, 54, 67, 92, 93, 94]
    out_list = [1] * 21
    default_value = 0
    jrc_tmf_disturbed = jrc_tmf_transitions.remap(in_list, out_list, default_value)
    return jrc_tmf_disturbed.rename("TMF_disturbed")

def jrc_tmf_plantations_prep():
    jrc_tmf_transitions_raw = ee.ImageCollection('projects/JRC/TMF/v1_2020/TransitionMap_Subtypes')
    jrc_tmf_transitions = jrc_tmf_transitions_raw.mosaic()
    in_list = [81, 82, 83, 84, 85, 86]
    out_list = [1] * 6
    default_value = 0
    jrc_tmf_plantations = jrc_tmf_transitions.remap(in_list, out_list, default_value)
    return jrc_tmf_plantations.rename("TMF_plant")

def jrc_tmf_undisturbed_prep():
    jrc_tmf_transitions_raw = ee.ImageCollection('projects/JRC/TMF/v1_2020/TransitionMap_Subtypes')
    jrc_tmf_transitions = jrc_tmf_transitions_raw.mosaic()
    in_list = [10, 11, 12]
    out_list = [1] * 3
    default_value = 0
    jrc_tmf_undisturbed = jrc_tmf_transitions.remap(in_list, out_list, default_value)
    return jrc_tmf_undisturbed.rename("TMF_undist")

def eth_kalischek_cocoa_prep():
    return ee.Image('projects/ee-nk-cocoa/assets/cocoa_map_threshold_065').rename("Cocoa_ETH")

def wur_radd_alerts_prep():
    # from datetime import datetime
    how_many_days_back = -(365 * 2)
    now = ee.Date(datetime.now())
    start_date = now.advance(how_many_days_back, 'day')
    start_date_yyddd = ee.Number.parse(start_date.format('yyDDD'))
    radd = ee.ImageCollection('projects/radar-wur/raddalert/v1')
    latest_radd_alert = radd.filterMetadata('layer', 'contains', 'alert').sort('system:time_end', False).mosaic()
    latest_radd_alert_confirmed_recent = latest_radd_alert.select('Date').gte(start_date_yyddd).selfMask()
    return latest_radd_alert_confirmed_recent.rename("RADD_alerts")

def fdap_palm_prep():
    fdap_palm2020_model_raw = ee.ImageCollection("projects/forestdatapartnership/assets/palm/palm_2020_model_20231026")
    fdap_palm = fdap_palm2020_model_raw.mosaic().gt(0.9).selfMask()
    return fdap_palm.rename("Oil_palm_FDaP")

def wcmc_wdpa_protection_prep():
    wdpa_poly = ee.FeatureCollection("WCMC/WDPA/current/polygons")
    template_image = ee.Image("UMD/hansen/global_forest_change_2022_v1_10")
    wdpa_filt = wdpa_poly.filter(
        ee.Filter.And(ee.Filter.neq('STATUS','Proposed'), 
                      ee.Filter.neq('STATUS', 'Not Reported'), 
                      ee.Filter.neq('DESIG_ENG', 'UNESCO-MAB Biosphere Reserve'))
                                )

    #turn into image (no crs etc set currently)
    wdpa_overlap = wdpa_filt.reduceToImage(['STATUS_YR'],'min');  #make into raster - remove mask if want 0s

    #make binary
    wdpa_binary = wdpa_overlap.lt(2070)#.unmask()
    
    wdpa_binary_reproj = reproject_to_template(wdpa_binary,template_image)

    return wdpa_binary_reproj.rename("WDPA")


def esa_worldcover_trees_prep():
    esa_worldcover_2020_raw = ee.Image("ESA/WorldCover/v100/2020");
    
    esa_worldcover_trees_2020 = esa_worldcover_2020_raw.eq(95).Or(esa_worldcover_2020_raw.eq(10)) #get trees and mnangroves
    
    return esa_worldcover_trees_2020.rename('ESA_TC_2020') 


def get_gaul_info(geometry):
    gaul2 = ee.FeatureCollection("FAO/GAUL/2015/level2");
    polygonsIntersectPoint = gaul2.filterBounds(geometry);
    return	ee.Algorithms.If( polygonsIntersectPoint.size().gt(0), polygonsIntersectPoint.first().toDictionary().select(["ADM0_NAME","ADM1_NAME", "ADM2_NAME"]) ,	None );

def get_gadm_info(geometry):
    gadm = ee.FeatureCollection("projects/ee-andyarnellgee/assets/p0004_commodity_mapper_support/raw/gadm_41_level_1")
    polygonsIntersectPoint = gadm.filterBounds(geometry);
    return	ee.Algorithms.If(polygonsIntersectPoint.size().gt(0), polygonsIntersectPoint.first().toDictionary().select(["GID_0","COUNTRY"]) ,	None );


###################
    
def get_stats(feature_or_feature_col):
    # Check if the input is a Feature or a FeatureCollection
    if isinstance(feature_or_feature_col, ee.Feature):
        # If the input is a Feature, call the server-side function for processing
        print("feature")
        output = ee.FeatureCollection([get_stats_feature(feature_or_feature_col)])
    elif isinstance(feature_or_feature_col, ee.FeatureCollection):
        # If the input is a FeatureCollection, call the server-side function for processing
        output = get_stats_fc(feature_or_feature_col)
    else:
        output = "Check inputs: not an ee.Feature or ee.FeatureCollection"
    return output



def get_stats_fc(feature_col):
    return ee.FeatureCollection(feature_col.map(get_stats_feature))



def get_stats_feature(feature):

    # Initialize an image with a constant band to start with
    img_combined = ee.Image(1).rename("Area_ha")

    # Add bands from each dataset
    img_combined = img_combined.addBands(creaf_descals_palm_prep())
    img_combined = img_combined.addBands(jaxa_forest_prep())
    # img_combined = img_combined.addBands(esri_lulc_trees_prep())
    img_combined = img_combined.addBands(glad_gfc_10pc_prep())
    img_combined = img_combined.addBands(glad_lulc_stable_prep())
    img_combined = img_combined.addBands(glad_pht_prep())
    img_combined = img_combined.addBands(jrc_gfc_2020_prep())
    img_combined = img_combined.addBands(fdap_palm_prep())
    img_combined = img_combined.addBands(jrc_tmf_disturbed_prep())
    img_combined = img_combined.addBands(jrc_tmf_plantations_prep())
    img_combined = img_combined.addBands(jrc_tmf_undisturbed_prep())
    img_combined = img_combined.addBands(eth_kalischek_cocoa_prep())
    img_combined = img_combined.addBands(wur_radd_alerts_prep())
    img_combined = img_combined.addBands(wcmc_wdpa_protection_prep())
    img_combined = img_combined.addBands(esa_worldcover_trees_prep())

    img_combined = img_combined.multiply(ee.Image.pixelArea())

    reduce = img_combined.reduceRegion(
        reducer=ee.Reducer.sum(), 
        geometry=feature.geometry(), 
        scale=10, 
        maxPixels=1e10,
        tileScale=4
    )
    
    # location = ee.Dictionary(get_gaul_info(feature.geometry()))
    
    # country = ee.Dictionary({'Country': location.get('ADM0_NAME')})
    
    location = ee.Dictionary(get_gadm_info(feature.geometry().centroid(1)))
    
    country = ee.Dictionary({'Country': location.get('GID_0')})

    reduce_ha = reduce.map(lambda key, val:
      ee.Number(val).divide(ee.Number(1e4)));
    
    area_ha = ee.Number(ee.Dictionary(reduce_ha).get("Area_ha"))
    
    percent_of_plot = reduce_ha.map(lambda key, val:
      ee.Number(val).divide(ee.Number(area_ha)).multiply(ee.Number(100)))

    percent_of_plot = percent_of_plot.set("Area_ha", area_ha.format('%.1f'))
    
    # properties = country.combine(ee.Dictionary(reduce_ha))
    
    properties = country.combine(ee.Dictionary(percent_of_plot))
        
    return feature.set(properties).setGeometry(None) 


def add_id_to_feature_collection(dataset,id_name="PLOTID"):
    """
    Adds an incremental (1,2,3 etc) 'id' property to each feature in the given FeatureCollection.

    Args:
    - dataset: ee.FeatureCollection, the FeatureCollection to operate on.

    Returns:
    - dataset_with_id: ee.FeatureCollection, the FeatureCollection with 'id' property added to each feature.
    """
    # Get the list of system:index values
    indexes = dataset.aggregate_array('system:index')
    
    # Create a sequence of numbers starting from 1 to the size of indexes
    ids = ee.List.sequence(1, indexes.size())
    
    # Create a dictionary mapping system:index to id
    id_by_index = ee.Dictionary.fromLists(indexes, ids)
    
    # Function to add 'id' property to each feature
    def add_id(feature):
        # Get the system:index of the feature
        system_index = feature.get('system:index')
        
        # Get the id corresponding to the system:index
        feature_id = id_by_index.get(system_index)
        
        # Set the 'id' property of the feature
        return feature.set(id_name, feature_id)
    
    # Map the add_id function over the dataset
    dataset_with_id = dataset.map(add_id)
    
    return dataset_with_id


    
def reformat_whisp_fc(feature_collection, 
                                order=None, 
                                id_name=None, 
                                flag_positive=None, 
                                round_properties=None, 
                                exclude_properties=None,
                                select_and_rename=None):
    """
    Process a FeatureCollection with various reformatting operations.

    Args:
    - feature_collection: ee.FeatureCollection, the FeatureCollection to operate on.
    - order: list, optional. Desired order of properties.
    - id_name: str, optional. Name of the ID property.
    - flag_positive: list, optional. List of property names to flag positive values.
    - round_properties: list, optional. List of property names to round to whole numbers.
    - exclude_properties: list, optional. List of property names to exclude.
    - select_and_rename: bool, optional. If True, select and rename properties.

    Returns:
    - processed_features: ee.FeatureCollection, FeatureCollection after processing.
    """


    # Reorder properties if specified
    if order:
        feature_collection = feature_collection.map(lambda feature: reorder_properties(feature, order))

    if id_name:
        feature_collection = add_id_to_feature_collection(feature_collection,id_name)

    # Flag positive values if specified
    if flag_positive:
        feature_collection = feature_collection.map(lambda feature: flag_positive_values(feature, flag_positive))

    # Round properties to whole numbers if specified
    if round_properties:
        feature_collection = feature_collection.map(lambda feature: round_properties_to_whole_numbers(feature, round_properties))

    # Exclude properties if specified
    if exclude_properties:
        feature_collection = feature_collection.map(lambda feature: copy_properties_and_exclude(feature, exclude_properties))
        
    #Function to select and rename properties
    def select_and_rename_properties(feature):
        first_feature = ee.Feature(feature_collection.first())
        property_names = first_feature.propertyNames().getInfo()
        new_property_names = [prop.replace('_', ' ') for prop in property_names]
        return feature.select(property_names, new_property_names)
        
    # Select and rename properties if specified
    if select_and_rename:
        
        feature_collection = feature_collection.map(select_and_rename_properties)

    
    return feature_collection


# def get_stats_formatted(feature_or_feature_col, 
#                                 order=None, 
#                                 id_name=None, 
#                                 flag_positive=None, 
#                                 round_properties=None, 
#                                 exclude_properties=None,
#                                 select_and_rename=None)

#     fc = get_stats(feature_or_feature_col)
    
#     fc_formatted = reformat_whisp_fc(fc, 
#                                     order=order, 
#                                     id_name=id_name, 
#                                     flag_positive=flag_positive, 
#                                     round_properties=round_properties, 
#                                     exclude_properties=exclude_properties,
#                                     select_and_rename=select_and_rename)
#     return fc_formatted

def stats_formatter_decorator(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> ee.FeatureCollection:
        feature_or_feature_col = args[0]
        fc = func(*args, **kwargs)
        fc_formatted = reformat_whisp_fc(
            fc,
            order=kwargs.get('order'),
            id_name=kwargs.get('id_name'),
            flag_positive=kwargs.get('flag_positive'),
            round_properties=kwargs.get('round_properties'),
            exclude_properties=kwargs.get('exclude_properties'),
            select_and_rename=kwargs.get('select_and_rename')
        )
        return fc_formatted
    return wrapper

@stats_formatter_decorator
def get_stats_formatted(feature_or_feature_col, **kwargs) -> ee.FeatureCollection:
    fc = get_stats(feature_or_feature_col)
    return fc