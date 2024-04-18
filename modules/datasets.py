import ee

from datetime import datetime


#add datasets below

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
    gfc = ee.Image("UMD/hansen/global_forest_change_2023_v1_11")
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
    gfc = ee.Image('UMD/hansen/global_forest_change_2023_v1_11')
    gfc_loss2001_2020 = gfc.select(['lossyear']).lte(20)
    return primary_ht_forests2001.where(gfc_loss2001_2020.eq(1), 0).rename("GLAD_Primary")

def jrc_gfc_2020_prep():
    jrc_gfc2020_raw = ee.ImageCollection("JRC/GFC2020/V1")
    return jrc_gfc2020_raw.mosaic().rename("EUFO_2020")

def jrc_tmf_transition_prep():
    jrc_tmf_transitions_raw = ee.ImageCollection('projects/JRC/TMF/v1_2020/TransitionMap_Subtypes')
    jrc_tmf_transitions = jrc_tmf_transitions_raw.mosaic()
    default_value = 0
    
    in_list_dist = [21, 22, 23, 24, 25, 26, 61, 62, 31, 32, 33, 63, 64, 51, 52, 53, 54, 67, 92, 93, 94]
    jrc_tmf_disturbed   = jrc_tmf_transitions.remap(in_list_dist, [1] * len(in_list_dist), default_value).rename("TMF_disturbed")

    in_list_plnt = [81, 82, 83, 84, 85, 86]
    jrc_tmf_plantations = jrc_tmf_transitions.remap(in_list_plnt, [1] * len(in_list_plnt), default_value).rename("TMF_plant")

    in_list_udis = [10, 11, 12]
    jrc_tmf_undisturbed = jrc_tmf_transitions.remap(in_list_udis, [1] * len(in_list_udis), default_value).rename("TMF_undist")
    
    jrc_tmf_transition = jrc_tmf_disturbed.addBands(jrc_tmf_plantations).addBands(jrc_tmf_undisturbed)
    return jrc_tmf_transition
    

def eth_kalischek_cocoa_prep():
    return ee.Image('projects/ee-nk-cocoa/assets/cocoa_map_threshold_065').rename("Cocoa_ETH")

    
def fdap_palm_prep():
    fdap_palm2020_model_raw = ee.ImageCollection("projects/forestdatapartnership/assets/palm/palm_2020_model_20231026")
    fdap_palm = fdap_palm2020_model_raw.mosaic().gt(0.9).selfMask()
    return fdap_palm.rename("Oil_palm_FDaP")


def radd_year_prep():
    from datetime import datetime
    radd = ee.ImageCollection('projects/radar-wur/raddalert/v1')
    
    radd_date = radd.filterMetadata('layer', 'contains', 'alert').select('Date').mosaic()
    # date of avaialbility 
    start_year = 19 ## (starts 2019 in Africa, then 2020 for S America and Asia: https://data.globalforestwatch.org/datasets/gfw::deforestation-alerts-radd/about
        
    current_year = datetime.now().year % 100 # NB the % 100 part gets last two digits needed 
        
    img_stack = None
    # Generate an image based on GFC with one band of forest tree loss per year from 2001 to 2022
    for year in range(start_year, current_year +1 ):
        #gfc_loss_year = gfc.select(['lossyear']).eq(i).And(gfc.select(['treecover2000']).gt(10))
        start = year*1000
        end   = year*1000+365
        radd_year = radd_date.updateMask(radd_date.gte(start)).updateMask(radd_date.lte(end)).gt(0).rename("radd_year_" + str(year))
        
        if img_stack is None:
            img_stack = radd_year
        else:
            img_stack = img_stack.addBands(radd_year)
    return img_stack


def wcmc_wdpa_protection_prep():
    wdpa_poly = ee.FeatureCollection("WCMC/WDPA/current/polygons")
    
    wdpa_filt = wdpa_poly.filter(
        ee.Filter.And(ee.Filter.neq('STATUS','Proposed'), 
                      ee.Filter.neq('STATUS', 'Not Reported'), 
                      ee.Filter.neq('DESIG_ENG', 'UNESCO-MAB Biosphere Reserve'))
                                )
    wdpa_binary = ee.Image().paint(wdpa_filt,1)
    
    return wdpa_binary.rename("WDPA")

def birdlife_kbas_biodiversity_prep():
    
    ##uploaded data - Non-commercial. For queries with limited numbers of sites. Exact number to be confirmed. 
    kbas_2023_poly = ee.FeatureCollection("projects/ee-andyarnellgee/assets/p0004_commodity_mapper_support/raw/KBAsGlobal_2023_March_01_POL");

    kba_2023_binary = ee.Image().paint(kbas_2023_poly,1)
    
    return kba_2023_binary.rename('KBA')


def feat_coll_prep(feats_name,attr_name, base_name):
    ## feats_name    = ee.FeatureCollection("projects/ee-cocoacmr/assets/feature_data/Aires_protegees_de_faunes");
    ## attr_name     = "desc_type"
    ## base_name     = "cmr"

    ## Load FC
    feats_poly    = ee.FeatureCollection(feats_name);
    
    ## Create unique list of values for selected attribute column
    list_types    = feats_poly.distinct(attr_name).aggregate_array(attr_name)
    list_length   = ee.Number(list_types.length()).toInt().getInfo()
    
    ## Initialize blank raster
    img_stack = None

    ## Add one band for each attribute values
    for i in range(0, list_length ):
        attr_type = list_types.get(i)
       
        feats_filter  = feats_poly.filter(ee.Filter.eq(attr_name,attr_type))
        feats_overlap = ee.Image().paint(feats_filter,1)
        feats_binary  = feats_overlap.gt(0).rename(base_name + "_" + str(i))

        if img_stack is None:
            img_stack = feats_binary
        else:
            img_stack = img_stack.addBands(feats_binary)
    
    return img_stack    #,list_types


def esa_worldcover_trees_prep():
    esa_worldcover_2020_raw = ee.Image("ESA/WorldCover/v100/2020");
    esa_worldcover_trees_2020 = esa_worldcover_2020_raw.eq(95).Or(esa_worldcover_2020_raw.eq(10)) #get trees and mnangroves
    return esa_worldcover_trees_2020.rename('ESA_TC_2020') 


def civ_ocs2020_prep():
    return ee.Image("projects/ee-bnetdcign2/assets/OCS_CI_2020vf").eq(9).rename("cocoa_bnetd") 

def tmf_loss_per_year():
    # Load the TMF Deforestation annual product
    tmf_def   = ee.ImageCollection('projects/JRC/TMF/v1_2022/DeforestationYear').mosaic()
    img_stack = None
    # Generate an image based on GFC with one band of forest tree loss per year from 2001 to 2022
    for i in range(0, 22 +1):
        tmf_def_year = tmf_def.eq(2000+i).rename("tmf_def_" + str(2000+i))
        if img_stack is None:
            img_stack = tmf_def_year
        else:
            img_stack = img_stack.addBands(tmf_def_year)
    return img_stack

def tmf_degr_per_year():
    # Load the TMF Degradation annual product
    tmf_def   = ee.ImageCollection('projects/JRC/TMF/v1_2022/DegradationYear').mosaic()
    img_stack = None
    # Generate an image based on GFC with one band of forest tree loss per year from 2001 to 2022
    for i in range(0, 22 +1):
        tmf_def_year = tmf_def.eq(2000+i).rename("tmf_deg_" + str(2000+i))
        if img_stack is None:
            img_stack = tmf_def_year
        else:
            img_stack = img_stack.addBands(tmf_def_year)
    return img_stack


def glad_gfc_loss_per_year():
    # Load the Global Forest Change dataset
    gfc = ee.Image("UMD/hansen/global_forest_change_2023_v1_11")
    img_stack = None
    # Generate an image based on GFC with one band of forest tree loss per year from 2001 to 2022
    for i in range(1, 23 +1):
        gfc_loss_year = gfc.select(['lossyear']).eq(i).And(gfc.select(['treecover2000']).gt(10))
        gfc_loss_year = gfc_loss_year.rename("GFC_Loss_Year_" + str(2000+i))
        if img_stack is None:
            img_stack = gfc_loss_year
        else:
            img_stack = img_stack.addBands(gfc_loss_year)
    return img_stack

def modis_fire_prep():
    modis_fire = ee.ImageCollection("MODIS/061/MCD64A1")
    img_stack = None
    
    start_year = 2000 ## (starts 2019 in Africa, then 2020 for S America and Asia: https://data.globalforestwatch.org/datasets/gfw::deforestation-alerts-radd/about
    
    current_year = datetime.now().year

    for year in range(start_year, current_year +1):
        date_st = str(year) + "-01-01"
        date_ed = str(year) + "-12-31"
        #print(date_st)
        modis_year = modis_fire.filterDate(date_st,date_ed).mosaic().select(['BurnDate']).gte(0).rename("modis_fire_" + str(year))
        
        if img_stack is None:
            img_stack = modis_year
        else:
            img_stack = img_stack.addBands(modis_year)
    return img_stack


def esa_fire_prep():
    esa_fire = ee.ImageCollection("ESA/CCI/FireCCI/5_1")
    img_stack = None
    for year in range(2001, 2020 +1):
        date_st = str(year) + "-01-01"
        date_ed = str(year) + "-12-31"
        #print(date_st)
        esa_year = esa_fire.filterDate(date_st,date_ed).mosaic().select(['BurnDate']).gte(0).rename("esa_fire_" + str(year))
        
        if img_stack is None:
            img_stack = esa_year
        else:
            img_stack = img_stack.addBands(esa_year)
    return img_stack

def combine_datasets():
    "Combines datasets into a single multiband image" 
    # Initialize an image with a constant band to start with
    img_combined = ee.Image(1).rename("Area_ha")

    # Add bands from each dataset
    img_combined = img_combined.addBands(creaf_descals_palm_prep())
    img_combined = img_combined.addBands(jaxa_forest_prep())
    img_combined = img_combined.addBands(esri_lulc_trees_prep())
    img_combined = img_combined.addBands(glad_gfc_10pc_prep())
    img_combined = img_combined.addBands(glad_lulc_stable_prep())
    img_combined = img_combined.addBands(glad_pht_prep())
    # img_combined = img_combined.addBands(jrc_gfc_2020_prep())
    img_combined = img_combined.addBands(fdap_palm_prep())
    img_combined = img_combined.addBands(jrc_tmf_transition_prep())
    img_combined = img_combined.addBands(eth_kalischek_cocoa_prep())
    # img_combined = img_combined.addBands(wur_radd_alerts_prep())
    img_combined = img_combined.addBands(wcmc_wdpa_protection_prep())
    img_combined = img_combined.addBands(birdlife_kbas_biodiversity_prep())
    img_combined = img_combined.addBands(esa_worldcover_trees_prep())
    img_combined = img_combined.addBands(civ_ocs2020_prep()) #
    img_combined = img_combined.addBands(tmf_loss_per_year()) # 
    img_combined = img_combined.addBands(tmf_degr_per_year()) #
    img_combined = img_combined.addBands(glad_gfc_loss_per_year()) #
    img_combined = img_combined.addBands(radd_year_prep())
    img_combined = img_combined.addBands(esa_fire_prep())
    img_combined = img_combined.addBands(modis_fire_prep())
    
    img_combined = img_combined.multiply(ee.Image.pixelArea())
    return img_combined