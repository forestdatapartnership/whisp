import ee

from datetime import datetime
from parameters.config_runtime import geometry_area_column

#add datasets below

# # Oil_palm_Descals 
# NB updated to Descals et al 2024 paper (as opposed to Descals et al 2021 paper)
def creaf_descals_palm_prep():
    # Load the Global Oil Palm Year of Plantation image and mosaic it
    img = ee.ImageCollection("projects/ee-globaloilpalm/assets/shared/GlobalOilPalm_YoP_2021").mosaic().select("minNBR_date")

    # Calculate the year of plantation and select all below and including 2020
    oil_palm_plantation_year = img.divide(365).add(1970).floor().lte(2020)

    # Create a mask for plantations in the year 2020 or earlier
    plantation_2020 = oil_palm_plantation_year.lte(2020).selfMask()
    return plantation_2020.rename("Oil_palm_Descals")

# JAXA_FNF_2020
def jaxa_forest_prep():
    jaxa_forest_non_forest_raw = ee.ImageCollection('JAXA/ALOS/PALSAR/YEARLY/FNF4')
    jaxa_forest_non_forest_2020 = jaxa_forest_non_forest_raw.filterDate('2020-01-01', '2020-12-31').select('fnf').mosaic()
    return jaxa_forest_non_forest_2020.lte(2).rename("JAXA_FNF_2020")


# GFC_TC_2020
def glad_gfc_10pc_prep():
    gfc = ee.Image("UMD/hansen/global_forest_change_2023_v1_11")
    gfc_treecover2000 = gfc.select(['treecover2000'])
    gfc_loss2001_2020 = gfc.select(['lossyear']).lte(20)
    gfc_treecover2020 = gfc_treecover2000.where(gfc_loss2001_2020.eq(1), 0)
    return gfc_treecover2020.gt(10).rename("GFC_TC_2020")

# GLAD_Primary
def glad_pht_prep():
    primary_ht_forests2001_raw = ee.ImageCollection('UMD/GLAD/PRIMARY_HUMID_TROPICAL_FORESTS/v1')
    primary_ht_forests2001 = primary_ht_forests2001_raw.select("Primary_HT_forests").mosaic().selfMask()
    gfc = ee.Image('UMD/hansen/global_forest_change_2023_v1_11')
    gfc_loss2001_2020 = gfc.select(['lossyear']).lte(20)
    return primary_ht_forests2001.where(gfc_loss2001_2020.eq(1), 0).rename("GLAD_Primary")

# EUFO_2020
def jrc_gfc_2020_prep():
    jrc_gfc2020_raw = ee.ImageCollection("JRC/GFC2020/V1")
    return jrc_gfc2020_raw.mosaic().rename("EUFO_2020")

# TMF_undist (undistrubed forest in 2020)
def jrc_tmf_undisturbed_prep():
    TMF_undist_2020 = ee.ImageCollection("projects/JRC/TMF/v1_2023/AnnualChanges").select("Dec2020").mosaic().eq(1) # update from https://github.com/forestdatapartnership/whisp/issues/42 
    return TMF_undist_2020.rename("TMF_undist")

#TMF_plant (plantations in 2020) 
def jrc_tmf_plantation_prep():
    transition = ee.ImageCollection('projects/JRC/TMF/v1_2023/TransitionMap_Subtypes').mosaic()
    deforestation_year = ee.ImageCollection('projects/JRC/TMF/v1_2023/DeforestationYear').mosaic()
    plantation = (transition.gte(81)).And(transition.lte(86))
    plantation_2020 = plantation.where(deforestation_year.gte(2021),0) # update from https://github.com/forestdatapartnership/whisp/issues/42 
    return plantation_2020.rename("TMF_plant");
    
# Cocoa_ETH
def eth_kalischek_cocoa_prep():
    return ee.Image('projects/ee-nk-cocoa/assets/cocoa_map_threshold_065').rename("Cocoa_ETH")

# Oil_palm_FDaP
def fdap_palm_prep():
    fdap_palm2020_model_raw = ee.ImageCollection("projects/forestdatapartnership/assets/palm/palm_2020_model_20240312")
    fdap_palm = fdap_palm2020_model_raw.mosaic().gt(0.95).selfMask() # to check with Nick (increased due to false postives)
    return fdap_palm.rename("Oil_palm_FDaP")

# RADD_year_2019 to RADD_year_< current year >
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
        end  = year*1000+365
        radd_year = radd_date.updateMask(radd_date.gte(start)).updateMask(radd_date.lte(end)).gt(0).rename("RADD_year_" +"20"+ str(year))
        
        if img_stack is None:
            img_stack = radd_year
        else:
            img_stack = img_stack.addBands(radd_year)
    return img_stack



# ESA_TC_2020
def esa_worldcover_trees_prep():
    esa_worldcover_2020_raw = ee.Image("ESA/WorldCover/v100/2020");
    esa_worldcover_trees_2020 = esa_worldcover_2020_raw.eq(95).Or(esa_worldcover_2020_raw.eq(10)) #get trees and mnangroves
    return esa_worldcover_trees_2020.rename('ESA_TC_2020') 

# Cocoa_bnetd
def civ_ocs2020_prep():
    return ee.Image("BNETD/land_cover/v1/2020").select("classification").eq(9).rename("Cocoa_bnetd") # cocoa from national land cover map for Côte d'Ivoire

# Rubber_RBGE  - from Royal Botanical Gardens of Edinburgh (RBGE) NB for 2021  
def rbge_rubber_prep():
    return ee.Image('users/wangyxtina/MapRubberPaper/rRubber10m202122_perc1585DifESAdist5pxPF').unmask().rename("Rubber_RBGE");


#### disturbances by year

# TMF_def_2000 to TMF_def_2022
def tmf_def_per_year_prep():
    # Load the TMF Deforestation annual product
    tmf_def   = ee.ImageCollection('projects/JRC/TMF/v1_2023/DeforestationYear').mosaic()
    img_stack = None
    # Generate an image based on GFC with one band of forest tree loss per year from 2001 to 2022
    for i in range(0, 22 +1):
        tmf_def_year = tmf_def.eq(2000+i).rename("TMF_def_" + str(2000+i))
        if img_stack is None:
            img_stack = tmf_def_year
        else:
            img_stack = img_stack.addBands(tmf_def_year)
    return img_stack


# TMF_deg_2000 to TMF_deg_2022 
def tmf_deg_per_year_prep():
    # Load the TMF Degradation annual product
    tmf_def   = ee.ImageCollection('projects/JRC/TMF/v1_2023/DegradationYear').mosaic()
    img_stack = None
    # Generate an image based on GFC with one band of forest tree loss per year from 2001 to 2022
    for i in range(0, 22 +1):
        tmf_def_year = tmf_def.eq(2000+i).rename("TMF_deg_" + str(2000+i))
        if img_stack is None:
            img_stack = tmf_def_year
        else:
            img_stack = img_stack.addBands(tmf_def_year)
    return img_stack

   
# GFC_loss_year_2001 to GFC_loss_year_2023 (correct for version 11)
def glad_gfc_loss_per_year_prep():
    # Load the Global Forest Change dataset
    gfc = ee.Image("UMD/hansen/global_forest_change_2023_v1_11")
    img_stack = None
    # Generate an image based on GFC with one band of forest tree loss per year from 2001 to 2022
    for i in range(1, 23 +1):
        gfc_loss_year = gfc.select(['lossyear']).eq(i).And(gfc.select(['treecover2000']).gt(10))
        gfc_loss_year = gfc_loss_year.rename("GFC_loss_year_" + str(2000+i))
        if img_stack is None:
            img_stack = gfc_loss_year
        else:
            img_stack = img_stack.addBands(gfc_loss_year)
    return img_stack


# MODIS_fire_2000 to MODIS_fire_< current year >
def modis_fire_prep():
    modis_fire = ee.ImageCollection("MODIS/061/MCD64A1")
    img_stack = None
    
    start_year = 2000 ## (starts 2019 in Africa, then 2020 for S America and Asia: https://data.globalforestwatch.org/datasets/gfw::deforestation-alerts-radd/about
    
    current_year = datetime.now().year

    for year in range(start_year, current_year +1):
        date_st = str(year) + "-01-01"
        date_ed = str(year) + "-12-31"
        #print(date_st)
        modis_year = modis_fire.filterDate(date_st,date_ed).mosaic().select(['BurnDate']).gte(0).rename("MODIS_fire_" + str(year))
        
        if img_stack is None:
            img_stack = modis_year
        else:
            img_stack = img_stack.addBands(modis_year)
    return img_stack



# ESA_fire_2000 to ESA_fire_2020
def esa_fire_prep():
    esa_fire = ee.ImageCollection("ESA/CCI/FireCCI/5_1")
    img_stack = None
    for year in range(2001, 2020 +1):
        date_st = str(year) + "-01-01"
        date_ed = str(year) + "-12-31"
        #print(date_st)
        esa_year = esa_fire.filterDate(date_st,date_ed).mosaic().select(['BurnDate']).gte(0).rename("ESA_fire_" + str(year))
        
        if img_stack is None:
            img_stack = esa_year
        else:
            img_stack = img_stack.addBands(esa_year)
    return img_stack



#### disturbances combined (split into before and after 2020) 

# RADD_after_2020
def radd_after_2020_prep():
    from datetime import datetime
    radd = ee.ImageCollection('projects/radar-wur/raddalert/v1')
    
    radd_date = radd.filterMetadata('layer', 'contains', 'alert').select('Date').mosaic()
    # date of avaialbility 
    start_year = 21 ## (starts 2019 in Africa, then 2020 for S America and Asia: https://data.globalforestwatch.org/datasets/gfw::deforestation-alerts-radd/about)
        
    current_year = datetime.now().year % 100 # NB the % 100 part gets last two digits needed        
    start = start_year*1000
    end  = current_year*1000+365
    return radd_date.updateMask(radd_date.gte(start)).updateMask(radd_date.lte(end)).gt(0).rename("RADD_after_2020")


# RADD_before_2020
def radd_before_2020_prep():
    from datetime import datetime
    radd = ee.ImageCollection('projects/radar-wur/raddalert/v1')
    
    radd_date = radd.filterMetadata('layer', 'contains', 'alert').select('Date').mosaic()
    # date of avaialbility 
    start_year = 19 ## (starts 2019 in Africa, then 2020 for S America and Asia: https://data.globalforestwatch.org/datasets/gfw::deforestation-alerts-radd/about)
        
    # current_year = datetime.now().year % 100 # NB the % 100 part gets last two digits needed 
    
    start = start_year*1000
    end  = 20*1000+365
    return radd_date.updateMask(radd_date.gte(start)).updateMask(radd_date.lte(end)).gt(0).rename("RADD_before_2020")


#TMF_deg_before_2020
def tmf_deg_before_2020_prep():
    tmf_deg = ee.ImageCollection('projects/JRC/TMF/v1_2023/DegradationYear').mosaic()
    return (tmf_deg.lte(2020)).And(tmf_deg.gte(2000)).rename("TMF_deg_before_2020")

#TMF_deg_after_2020
def tmf_deg_after_2020_prep():
    tmf_deg = ee.ImageCollection('projects/JRC/TMF/v1_2023/DegradationYear').mosaic()
    return tmf_deg.gt(2020).rename("TMF_deg_after_2020")

#tmf_def_before_2020
def tmf_def_before_2020_prep():
    tmf_def = ee.ImageCollection('projects/JRC/TMF/v1_2023/DeforestationYear').mosaic()
    return (tmf_def.lte(2020)).And(tmf_def.gte(2000)).rename("TMF_def_before_2020")

#tmf_def_after_2020
def tmf_def_after_2020_prep():
    tmf_def = ee.ImageCollection('projects/JRC/TMF/v1_2023/DeforestationYear').mosaic()
    return tmf_def.gt(2020).rename("TMF_def_after_2020")

# GFC_loss_before_2020 (loss within 10 percent cover; includes 2020; correct for version 11)
def glad_gfc_loss_before_2020_prep():
    # Load the Global Forest Change dataset
    gfc = ee.Image("UMD/hansen/global_forest_change_2023_v1_11")
    gfc_loss = gfc.select(['lossyear']).lte(20).And(gfc.select(['treecover2000']).gt(10))
    return gfc_loss.rename("GFC_loss_before_2020")
    
# GFC_loss_after_2020 (loss within 10 percent cover; correct for version 11)
def glad_gfc_loss_after_2020_prep():
    # Load the Global Forest Change dataset
    gfc = ee.Image("UMD/hansen/global_forest_change_2023_v1_11")
    gfc_loss = gfc.select(['lossyear']).gt(20).And(gfc.select(['treecover2000']).gt(10))
    return gfc_loss.rename("GFC_loss_after_2020")

# MODIS_fire_before_2020
def modis_fire_before_2020_prep():
    modis_fire = ee.ImageCollection("MODIS/061/MCD64A1")
    start_year = 2000 
    end_year = 2020
    date_st = str(start_year) + "-01-01"
    date_ed = str(end_year) + "-12-31"
    return modis_fire.filterDate(date_st,date_ed).mosaic().select(['BurnDate']).gte(0).rename("MODIS_fire_before_2020")

# MODIS_fire_after_2020
def modis_fire_after_2020_prep():
    modis_fire = ee.ImageCollection("MODIS/061/MCD64A1")
    start_year = 2021 
    end_year = datetime.now().year
    date_st = str(start_year) + "-01-01"
    date_ed = str(end_year) + "-12-31"
    return modis_fire.filterDate(date_st,date_ed).mosaic().select(['BurnDate']).gte(0).rename("MODIS_fire_after_2020")

# ESA_fire_before_2020
def esa_fire_before_2020_prep():
    esa_fire = ee.ImageCollection("ESA/CCI/FireCCI/5_1")
    start_year = 2000 
    end_year = 2020
    date_st = str(start_year) + "-01-01"
    date_ed = str(end_year) + "-12-31"
    return esa_fire.filterDate(date_st,date_ed).mosaic().select(['BurnDate']).gte(0).rename("ESA_fire_before_2020")

####### handling feature datasets 

def feat_coll_prep(feats_name,attr_name, base_name):
    ## feats_name    = ee.FeatureCollection(your_asset_id);
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


# WDPA 
# World Database on Protected areas 

# Temporarily removed - pending agreements for use in API. Results will likely only be included only in the Whisp API. 
# NB dataset is restricted for commercial use. Shown here using non-commercial release of the WDPA, for code tranparency only.
def wcmc_wdpa_protection_prep():
    wdpa_poly = ee.FeatureCollection("WCMC/WDPA/current/polygons")
    
    wdpa_filt = wdpa_poly.filter(
        ee.Filter.And(ee.Filter.neq('STATUS','Proposed'), 
                      ee.Filter.neq('STATUS', 'Not Reported'), 
                      ee.Filter.neq('DESIG_ENG', 'UNESCO-MAB Biosphere Reserve'))
                                )
    wdpa_binary = ee.Image().paint(wdpa_filt,1)
    
    return wdpa_binary.rename("WDPA")


# KBA  
# Key Biodiversity Areas (KBAs)

# Temporarily removed pending agreements for use in API.  Results will likely only be included only in the Whisp API.
# NB dataset is restricted for commercial use. Shown here for code tranparency.
# Results will be included only in the Whisp API where they will be restricted to a limited number of plots. 
def birdlife_kbas_biodiversity_prep():
    
    ##uploaded data - Non-commercial. For queries with limited numbers of sites. Exact number to be confirmed. 
    kbas_2023_poly = ee.FeatureCollection("projects/ee-andyarnellgee/assets/p0004_commodity_mapper_support/raw/KBAsGlobal_2023_March_01_POL");

    kba_2023_binary = ee.Image().paint(kbas_2023_poly,1)
    
    return kba_2023_binary.rename('KBA')





def try_access(asset_prep_func):
    try:
        return asset_prep_func()
    except Exception as e:
        print(f"Error accessing asset: {e}")
        return None

# Now, wrap each datasetfunction in try_access

def combine_datasets():
    "Combines datasets into a single multiband image" 
    # Initialize an image with a constant band to start with
    img_combined = ee.Image(1).rename(geometry_area_column) # becomes the area column after pixel area multiplication step below

    # Add bands from each dataset
    img_combined = img_combined.addBands(try_access(creaf_descals_palm_prep))
    img_combined = img_combined.addBands(try_access(jaxa_forest_prep))
    img_combined = img_combined.addBands(try_access(glad_gfc_10pc_prep))
    img_combined = img_combined.addBands(try_access(glad_pht_prep))
    img_combined = img_combined.addBands(try_access(jrc_gfc_2020_prep))
    img_combined = img_combined.addBands(try_access(fdap_palm_prep))
    img_combined = img_combined.addBands(try_access(jrc_tmf_undisturbed_prep))
    img_combined = img_combined.addBands(try_access(jrc_tmf_plantation_prep))
    img_combined = img_combined.addBands(try_access(eth_kalischek_cocoa_prep))
    # img_combined = img_combined.addBands(try_access(wcmc_wdpa_protection_prep))
    # img_combined = img_combined.addBands(try_access(birdlife_kbas_biodiversity_prep))
    img_combined = img_combined.addBands(try_access(esa_worldcover_trees_prep))
    img_combined = img_combined.addBands(try_access(civ_ocs2020_prep)) 
    img_combined = img_combined.addBands(try_access(rbge_rubber_prep))  
    img_combined = img_combined.addBands(try_access(tmf_def_per_year_prep)) # multi year
    img_combined = img_combined.addBands(try_access(tmf_deg_per_year_prep)) # multi year
    img_combined = img_combined.addBands(try_access(glad_gfc_loss_per_year_prep)) # multi year
    img_combined = img_combined.addBands(try_access(radd_year_prep)) # multi year
    img_combined = img_combined.addBands(try_access(esa_fire_prep)) # multi year
    img_combined = img_combined.addBands(try_access(modis_fire_prep)) # multi year
    img_combined = img_combined.addBands(try_access(glad_gfc_loss_before_2020_prep)) # multi year
    img_combined = img_combined.addBands(try_access(glad_gfc_loss_after_2020_prep)) # multi year
    img_combined = img_combined.addBands(try_access(esa_fire_before_2020_prep)) # combined 
    img_combined = img_combined.addBands(try_access(modis_fire_before_2020_prep)) # combined
    img_combined = img_combined.addBands(try_access(modis_fire_after_2020_prep)) # combined
    img_combined = img_combined.addBands(try_access(tmf_def_before_2020_prep)) # combined
    img_combined = img_combined.addBands(try_access(tmf_def_after_2020_prep))# combined
    img_combined = img_combined.addBands(try_access(tmf_deg_before_2020_prep)) # combined
    img_combined = img_combined.addBands(try_access(tmf_deg_after_2020_prep)) # combined
    img_combined = img_combined.addBands(try_access(radd_after_2020_prep)) # combined
    img_combined = img_combined.addBands(try_access(radd_before_2020_prep)) # combined

    
    
    img_combined = img_combined.multiply(ee.Image.pixelArea()) # multiple all bands by pixel area
    return img_combined
