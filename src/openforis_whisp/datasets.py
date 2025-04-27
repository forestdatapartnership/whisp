import ee

# ee.Authenticate()
# ee.Initialize()

from datetime import datetime

# from openforis_whisp.parameters.config_runtime import (
#     geometry_area_column,
# )  # ideally make relative import statement

# defining here instead of importing from config_runtime, to allow functioning as more of a standalone script
geometry_area_column = "Area"

import inspect

import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_logger(name):
    return logging.getLogger(name)


# Add datasets below

# tree cover datasets


# ESA_TC_2020
def esa_worldcover_trees_prep():
    esa_worldcover_2020_raw = ee.Image("ESA/WorldCover/v100/2020")
    esa_worldcover_trees_2020 = esa_worldcover_2020_raw.eq(95).Or(
        esa_worldcover_2020_raw.eq(10)
    )  # get trees and mnangroves
    return esa_worldcover_trees_2020.rename("ESA_TC_2020")


# EUFO_2020
def jrc_gfc_2020_prep():
    jrc_gfc2020_raw = ee.ImageCollection("JRC/GFC2020/V2")
    return jrc_gfc2020_raw.mosaic().rename("EUFO_2020")


# JAXA_FNF_2020
def jaxa_forest_prep():
    jaxa_forest_non_forest_raw = ee.ImageCollection("JAXA/ALOS/PALSAR/YEARLY/FNF4")
    jaxa_forest_non_forest_2020 = (
        jaxa_forest_non_forest_raw.filterDate("2020-01-01", "2020-12-31")
        .select("fnf")
        .mosaic()
    )
    return jaxa_forest_non_forest_2020.lte(2).rename("JAXA_FNF_2020")


# GFC_TC_2020
def glad_gfc_10pc_prep():
    gfc = ee.Image("UMD/hansen/global_forest_change_2023_v1_11")
    gfc_treecover2000 = gfc.select(["treecover2000"])
    gfc_loss2001_2020 = gfc.select(["lossyear"]).lte(20)
    gfc_treecover2020 = gfc_treecover2000.where(gfc_loss2001_2020.eq(1), 0)
    return gfc_treecover2020.gt(10).rename("GFC_TC_2020")


# GLAD_Primary
def glad_pht_prep():
    primary_ht_forests2001_raw = ee.ImageCollection(
        "UMD/GLAD/PRIMARY_HUMID_TROPICAL_FORESTS/v1"
    )
    primary_ht_forests2001 = (
        primary_ht_forests2001_raw.select("Primary_HT_forests").mosaic().selfMask()
    )
    gfc = ee.Image("UMD/hansen/global_forest_change_2023_v1_11")
    gfc_loss2001_2020 = gfc.select(["lossyear"]).lte(20)
    return primary_ht_forests2001.where(gfc_loss2001_2020.eq(1), 0).rename(
        "GLAD_Primary"
    )


# TMF_undist (undistrubed forest in 2020)
def jrc_tmf_undisturbed_prep():
    TMF_undist_2020 = (
        ee.ImageCollection("projects/JRC/TMF/v1_2023/AnnualChanges")
        .select("Dec2020")
        .mosaic()
        .eq(1)
    )  # update from https://github.com/forestdatapartnership/whisp/issues/42
    return TMF_undist_2020.rename("TMF_undist")


# Forest Persistence FDaP
def fdap_forest_prep():
    fdap_forest_raw = ee.Image(
        "projects/forestdatapartnership/assets/community_forests/ForestPersistence_2020"
    )
    fdap_forest = fdap_forest_raw.gt(0.75)
    return fdap_forest.rename("Forest_FDaP")


#########################primary forest
# EUFO JRC Global forest type - primary
def gft_primary_prep():
    gft_raw = ee.ImageCollection("JRC/GFC2020_subtypes/V0").mosaic()
    gft_primary = gft_raw.eq(10)
    return gft_primary.rename("GFT_primary")


# Intact Forest Landscape 2020
def IFL_2020_prep():
    IFL_2020 = ee.Image("users/potapovpeter/IFL_2020")
    return IFL_2020.rename("IFL_2020")


# European Primary Forest Dataset
def EPFD_prep():
    EPFD = ee.FeatureCollection("HU_BERLIN/EPFD/V2/polygons")
    EPFD_binary = ee.Image().paint(EPFD, 1)
    return EPFD_binary.rename("European_Primary_Forest")


# EUFO JRC Global forest type - naturally regenerating planted/plantation forests
def gft_nat_reg_prep():
    gft_raw = ee.ImageCollection("JRC/GFC2020_subtypes/V0").mosaic()
    gft_nat_reg = gft_raw.eq(1)
    return gft_nat_reg.rename("GFT_naturally_regenerating")


#########################planted and plantation forests

# EUFO JRC Global forest type - planted/plantation forests
def gft_plantation_prep():
    gft_raw = ee.ImageCollection("JRC/GFC2020_subtypes/V0").mosaic()
    gft_plantation = gft_raw.eq(20)
    return gft_plantation.rename("GFT_planted_plantation")


def IIASA_planted_prep():
    iiasa = ee.Image("projects/sat-io/open-datasets/GFM/FML_v3-2")
    iiasa_PL = iiasa.eq(31).Or(iiasa.eq(32))
    return iiasa_PL.rename("IIASA_planted_plantation")


#########################TMF regrowth in 2023
def tmf_regrowth_prep():
    # Load the TMF Degradation annual product
    TMF_AC = ee.ImageCollection("projects/JRC/TMF/v1_2023/AnnualChanges").mosaic()
    TMF_AC_2023 = TMF_AC.select("Dec2023")
    Regrowth_TMF = TMF_AC_2023.eq(4)
    return Regrowth_TMF.rename("TMF_regrowth_2023")


############tree crops

# TMF_plant (plantations in 2020)
def jrc_tmf_plantation_prep():
    transition = ee.ImageCollection(
        "projects/JRC/TMF/v1_2023/TransitionMap_Subtypes"
    ).mosaic()
    deforestation_year = ee.ImageCollection(
        "projects/JRC/TMF/v1_2023/DeforestationYear"
    ).mosaic()
    plantation = (transition.gte(81)).And(transition.lte(86))
    plantation_2020 = plantation.where(
        deforestation_year.gte(2021), 0
    )  # update from https://github.com/forestdatapartnership/whisp/issues/42
    return plantation_2020.rename("TMF_plant")


# # Oil_palm_Descals
# NB updated to Descals et al 2024 paper (as opposed to Descals et al 2021 paper)
def creaf_descals_palm_prep():
    # Load the Global Oil Palm Year of Plantation image and mosaic it
    img = (
        ee.ImageCollection(
            "projects/ee-globaloilpalm/assets/shared/GlobalOilPalm_YoP_2021"
        )
        .mosaic()
        .select("minNBR_date")
    )

    # Calculate the year of plantation and select all below and including 2020
    oil_palm_plantation_year = img.divide(365).add(1970).floor().lte(2020)

    # Create a mask for plantations in the year 2020 or earlier
    plantation_2020 = oil_palm_plantation_year.lte(2020).selfMask()
    return plantation_2020.rename("Oil_palm_Descals")

    # Calculate the year of plantation
    oil_palm_plantation_year = img.divide(365).add(1970).floor().lte(2020)

    # Create a mask for plantations in the year 2020 or earlier
    plantation_2020 = oil_palm_plantation_year.lte(2020).selfMask()
    return plantation_2020.rename("Oil_palm_Descals")


# Cocoa_ETH
def eth_kalischek_cocoa_prep():
    return ee.Image("projects/ee-nk-cocoa/assets/cocoa_map_threshold_065").rename(
        "Cocoa_ETH"
    )


# Oil Palm FDaP
def fdap_palm_prep():
    fdap_palm2020_model_raw = ee.ImageCollection(
        "projects/forestdatapartnership/assets/palm/model_2024a"
    )
    fdap_palm = (
        fdap_palm2020_model_raw.filterDate("2020-01-01", "2020-12-31")
        .mosaic()
        .gt(0.83)  # Threshold for Oil Palm
    )
    return fdap_palm.rename("Oil_palm_FDaP")


def fdap_palm_2023_prep():
    fdap_palm2020_model_raw = ee.ImageCollection(
        "projects/forestdatapartnership/assets/palm/model_2024a"
    )
    fdap_palm = (
        fdap_palm2020_model_raw.filterDate("2023-01-01", "2023-12-31")
        .mosaic()
        .gt(0.83)  # Threshold for Oil Palm
    )
    return fdap_palm.rename("Oil_palm_2023_FDaP")


# Rubber FDaP
def fdap_rubber_prep():
    fdap_rubber2020_model_raw = ee.ImageCollection(
        "projects/forestdatapartnership/assets/rubber/model_2024a"
    )
    fdap_rubber = (
        fdap_rubber2020_model_raw.filterDate("2020-01-01", "2020-12-31")
        .mosaic()
        .gt(0.93)  # Threshold for Rubber
    )
    return fdap_rubber.rename("Rubber_FDaP")


def fdap_rubber_2023_prep():
    fdap_rubber2020_model_raw = ee.ImageCollection(
        "projects/forestdatapartnership/assets/rubber/model_2024a"
    )
    fdap_rubber = (
        fdap_rubber2020_model_raw.filterDate("2023-01-01", "2023-12-31")
        .mosaic()
        .gt(0.93)  # Threshold for Rubber
    )
    return fdap_rubber.rename("Rubber_2023_FDaP")


# Cocoa FDaP
def fdap_cocoa_prep():
    fdap_cocoa2020_model_raw = ee.ImageCollection(
        "projects/forestdatapartnership/assets/cocoa/model_2024a"
    )
    fdap_cocoa = (
        fdap_cocoa2020_model_raw.filterDate("2020-01-01", "2020-12-31")
        .mosaic()
        .gt(0.5)  # Threshold for Cocoa
    )
    return fdap_cocoa.rename("Cocoa_FDaP")


def fdap_cocoa_2023_prep():
    fdap_cocoa2020_model_raw = ee.ImageCollection(
        "projects/forestdatapartnership/assets/cocoa/model_2024a"
    )
    fdap_cocoa = (
        fdap_cocoa2020_model_raw.filterDate("2023-01-01", "2023-12-31")
        .mosaic()
        .gt(0.5)  # Threshold for Cocoa
    )
    return fdap_cocoa.rename("Cocoa_2023_FDaP")


# Cocoa_bnetd
def civ_ocs2020_prep():
    return (
        ee.Image("BNETD/land_cover/v1/2020")
        .select("classification")
        .eq(9)
        .rename("Cocoa_bnetd")
    )  # cocoa from national land cover map for Côte d'Ivoire


# Rubber_RBGE  - from Royal Botanical Gardens of Edinburgh (RBGE) NB for 2021
def rbge_rubber_prep():
    return (
        ee.Image(
            "users/wangyxtina/MapRubberPaper/rRubber10m202122_perc1585DifESAdist5pxPF"
        )
        .unmask()
        .rename("Rubber_RBGE")
    )


################## seasonal crops

# soy 2020 Brazil
def soy_song_2020_prep():
    return ee.Image("projects/glad/soy_annual_SA/2020").unmask().rename("Soy_Song_2020")


##############2023
# ESRI 2023
# ESRI 2023 - Tree Cover
def esri_2023_TC_prep():
    esri_lulc10_raw = ee.ImageCollection(
        "projects/sat-io/open-datasets/landcover/ESRI_Global-LULC_10m_TS"
    )
    esri_lulc10_TC = (
        esri_lulc10_raw.filterDate("2023-01-01", "2023-12-31").mosaic().eq(2)
    )
    return esri_lulc10_TC.rename("ESRI_2023_TC")


# ESRI 2023 - Crop
def esri_2023_crop_prep():
    esri_lulc10_raw = ee.ImageCollection(
        "projects/sat-io/open-datasets/landcover/ESRI_Global-LULC_10m_TS"
    )
    esri_lulc10_crop = (
        esri_lulc10_raw.filterDate("2023-01-01", "2023-12-31").mosaic().eq(5)
    )
    return esri_lulc10_crop.rename("ESRI_2023_crop")


# GLC_FCS30D 2022

# GLC_FCS30D Tree Cover
# forest classes + swamp + mangrove / what to do with shrubland?
def GLC_FCS30D_TC_2022_prep():
    GLC_FCS30D = (
        ee.ImageCollection("projects/sat-io/open-datasets/GLC-FCS30D/annual")
        .mosaic()
        .select(22)
    )
    GLC_FCS30D_TC = (
        (GLC_FCS30D.gte(51))
        .And(GLC_FCS30D.lte(92))
        .Or(GLC_FCS30D.eq(181))
        .Or(GLC_FCS30D.eq(185))
    )
    return GLC_FCS30D_TC.rename("GLC_FCS30D_TC_2022")


# GLC_FCS30D crop
# 10	Rainfed cropland; 11	Herbaceous cover; 12	Tree or shrub cover (Orchard); 20	Irrigated cropland
def GLC_FCS30D_crop_2022_prep():
    GLC_FCS30D = (
        ee.ImageCollection("projects/sat-io/open-datasets/GLC-FCS30D/annual")
        .mosaic()
        .select(22)
    )
    GLC_FCS30D_crop = GLC_FCS30D.gte(10).And(GLC_FCS30D.lte(20))
    return GLC_FCS30D_crop.rename("GLC_FCS30D_crop_2022")


#### disturbances by year

# RADD_year_2019 to RADD_year_< current year >
def radd_year_prep():
    from datetime import datetime

    radd = ee.ImageCollection("projects/radar-wur/raddalert/v1")

    radd_date = (
        radd.filterMetadata("layer", "contains", "alert").select("Date").mosaic()
    )
    # date of avaialbility
    start_year = 19  ## (starts 2019 in Africa, then 2020 for S America and Asia: https://data.globalforestwatch.org/datasets/gfw::deforestation-alerts-radd/about

    current_year = (
        datetime.now().year
        % 100
        # NB the % 100 part gets last two digits needed
    )

    img_stack = None
    # Generate an image based on GFC with one band of forest tree loss per year from 2001 to <current year>
    for year in range(start_year, current_year + 1):
        # gfc_loss_year = gfc.select(['lossyear']).eq(i).And(gfc.select(['treecover2000']).gt(10)) # use any definition of loss
        start = year * 1000
        end = year * 1000 + 365
        radd_year = (
            radd_date.updateMask(radd_date.gte(start))
            .updateMask(radd_date.lte(end))
            .gt(0)
            .rename("RADD_year_" + "20" + str(year))
        )

        if img_stack is None:
            img_stack = radd_year
        else:
            img_stack = img_stack.addBands(radd_year)
    return img_stack


# TMF_def_2000 to TMF_def_2023
def tmf_def_per_year_prep():
    # Load the TMF Deforestation annual product
    tmf_def = ee.ImageCollection("projects/JRC/TMF/v1_2023/DeforestationYear").mosaic()
    img_stack = None
    # Generate an image based on GFC with one band of forest tree loss per year from 2001 to 2022
    for i in range(0, 23 + 1):
        tmf_def_year = tmf_def.eq(2000 + i).rename("TMF_def_" + str(2000 + i))
        if img_stack is None:
            img_stack = tmf_def_year
        else:
            img_stack = img_stack.addBands(tmf_def_year)
    return img_stack


# TMF_deg_2000 to TMF_deg_2023
def tmf_deg_per_year_prep():
    # Load the TMF Degradation annual product
    tmf_def = ee.ImageCollection("projects/JRC/TMF/v1_2023/DegradationYear").mosaic()
    img_stack = None
    # Generate an image based on GFC with one band of forest tree loss per year from 2001 to 2022
    for i in range(0, 23 + 1):
        tmf_def_year = tmf_def.eq(2000 + i).rename("TMF_deg_" + str(2000 + i))
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
    for i in range(1, 23 + 1):
        gfc_loss_year = (
            gfc.select(["lossyear"]).eq(i).And(gfc.select(["treecover2000"]).gt(10))
        )
        gfc_loss_year = gfc_loss_year.rename("GFC_loss_year_" + str(2000 + i))
        if img_stack is None:
            img_stack = gfc_loss_year
        else:
            img_stack = img_stack.addBands(gfc_loss_year)
    return img_stack


# MODIS_fire_2000 to MODIS_fire_< current year >
def modis_fire_prep():
    modis_fire = ee.ImageCollection("MODIS/061/MCD64A1")
    start_year = 2000

    # Determine the last available year by checking the latest image in the collection
    last_image = modis_fire.sort("system:time_start", False).first()
    last_date = ee.Date(last_image.get("system:time_start"))
    end_year = last_date.get("year").getInfo()

    img_stack = None

    for year in range(start_year, end_year + 1):
        date_st = f"{year}-01-01"
        date_ed = f"{year}-12-31"
        modis_year = (
            modis_fire.filterDate(date_st, date_ed)
            .mosaic()
            .select(["BurnDate"])
            .gte(0)
            .rename(f"MODIS_fire_{year}")
        )
        img_stack = modis_year if img_stack is None else img_stack.addBands(modis_year)

    return img_stack


# ESA_fire_2000 to ESA_fire_2020
def esa_fire_prep():
    esa_fire = ee.ImageCollection("ESA/CCI/FireCCI/5_1")
    start_year = 2001

    # Determine the last available year by checking the latest image in the collection
    last_image = esa_fire.sort("system:time_start", False).first()
    last_date = ee.Date(last_image.get("system:time_start"))
    end_year = last_date.get("year").getInfo()

    img_stack = None

    for year in range(start_year, end_year + 1):
        date_st = f"{year}-01-01"
        date_ed = f"{year}-12-31"
        esa_year = (
            esa_fire.filterDate(date_st, date_ed)
            .mosaic()
            .select(["BurnDate"])
            .gte(0)
            .rename(f"ESA_fire_{year}")
        )
        img_stack = esa_year if img_stack is None else img_stack.addBands(esa_year)

    return img_stack


# # DIST_alert_2024 to DIST_alert_< current year >
# # Notes:
# # 1) so far only available for 2024 onwards in GEE
# # TO DO - see if gee asset for pre 2020-2024 is available from GLAD team, else download from nasa and put in Whisp assets
# # 2) masked alerts (as dist alerts are for all vegetation) to JRC EUFO 2020 layer, as close to EUDR definition
# # TO DO - ask opinions on if others (such as treecover data from GLAD team) should be used instead


# def glad_dist_year_prep():

#     # Load the vegetation disturbance collections

#     #  Vegetation disturbance status (0-8, class flag, 8-bit)
#     VEGDISTSTATUS = ee.ImageCollection(
#         "projects/glad/HLSDIST/current/VEG-DIST-STATUS"
#     ).mosaic()
#     # Initial vegetation disturbance date (>0: days since 2020-12-31, 16-bit)
#     VEGDISTDATE = ee.ImageCollection(
#         "projects/glad/HLSDIST/current/VEG-DIST-DATE"
#     ).mosaic()

#     # NB relies on initial date of disturbance - consider if last date needed? : VEGLASTDATE = ee.ImageCollection("projects/glad/HLSDIST/current/VEG-LAST-DATE").mosaic(); # Last assessed observation date (≥1, days, 16-bit)

#     # Key for high-confidence alerts (values 3, 6, 7, 8)
#     high_conf_values = [3, 6, 7, 8]
#     # where:
#     # 3 = <50% loss, high confidence, ongoing
#     # 6 = ≥50% loss, high confidence, ongoing
#     # 7 = <50% loss, high confidence, finished
#     # 8 = ≥50% loss, high confidence, finished
#     # Note could use <50% loss (i.e. only 6 and 7) for if want to be more strict

#     # Create high-confidence mask
#     dist_high_conf = VEGDISTSTATUS.remap(
#         high_conf_values, [1] * len(high_conf_values), 0
#     )

#     # Determine start year and current year dynamically
#     start_year = 2024  # Set the first year of interest
#     current_year = datetime.now().year

#     # Calculate days since December 31, 2020 for start and end dates (server-side)
#     start_of_2020 = ee.Date("2020-12-31").millis().divide(86400000).int()

#     # Create a list to hold the yearly images
#     yearly_images = []

#     for year in range(start_year, current_year + 1):
#         start_of_year = (
#             ee.Date(f"{year}-01-01")
#             .millis()
#             .divide(86400000)
#             .int()
#             .subtract(start_of_2020)
#         )
#         start_of_next_year = (
#             ee.Date(f"{year + 1}-01-01")
#             .millis()
#             .divide(86400000)
#             .int()
#             .subtract(start_of_2020)
#         )

#         # Filter VEG-DIST-DATE for the selected year
#         dist_year = VEGDISTDATE.gte(start_of_year).And(
#             VEGDISTDATE.lt(start_of_next_year)
#         )

#         # Apply high-confidence mask and rename the band
#         high_conf_year = dist_year.updateMask(dist_high_conf).rename(
#             f"DIST_year_{year}"
#         )

#         # Append the year's data to the list
#         yearly_images.append(high_conf_year)

#     # Combine all yearly images into a single image
#     img_stack = ee.Image.cat(yearly_images)

#     # Rename the bands correctly
#     band_names = [f"DIST_year_{year}" for year in range(start_year, current_year + 1)]
#     img_stack = img_stack.select(img_stack.bandNames(), band_names)

#     return img_stack.updateMask(
#         jrc_gfc_2020_prep()
#     )  # mask yearly dist alerts to forest cover in 2020


#### disturbances combined (split into before and after 2020)

# RADD_after_2020
def radd_after_2020_prep():
    from datetime import datetime

    radd = ee.ImageCollection("projects/radar-wur/raddalert/v1")

    radd_date = (
        radd.filterMetadata("layer", "contains", "alert").select("Date").mosaic()
    )
    # date of avaialbility
    start_year = 21  ## (starts 2019 in Africa, then 2020 for S America and Asia: https://data.globalforestwatch.org/datasets/gfw::deforestation-alerts-radd/about)

    current_year = (
        datetime.now().year % 100
    )  # NB the % 100 part gets last two digits needed
    start = start_year * 1000
    end = current_year * 1000 + 365
    return (
        radd_date.updateMask(radd_date.gte(start))
        .updateMask(radd_date.lte(end))
        .gt(0)
        .rename("RADD_after_2020")
    )


# RADD_before_2020
def radd_before_2020_prep():
    from datetime import datetime

    radd = ee.ImageCollection("projects/radar-wur/raddalert/v1")

    radd_date = (
        radd.filterMetadata("layer", "contains", "alert").select("Date").mosaic()
    )
    # date of avaialbility
    start_year = 19  ## (starts 2019 in Africa, then 2020 for S America and Asia: https://data.globalforestwatch.org/datasets/gfw::deforestation-alerts-radd/about)

    # current_year = datetime.now().year % 100 # NB the % 100 part gets last two digits needed

    start = start_year * 1000
    end = 20 * 1000 + 365
    return (
        radd_date.updateMask(radd_date.gte(start))
        .updateMask(radd_date.lte(end))
        .gt(0)
        .rename("RADD_before_2020")
    )


# # DIST_after_2020
# # alerts only for after 2020 currently so need to use date
# def glad_dist_after_2020_prep():

#     # Load the vegetation disturbance collections
#     VEGDISTSTATUS = ee.ImageCollection(
#         "projects/glad/HLSDIST/current/VEG-DIST-STATUS"
#     ).mosaic()

#     # Key for high-confidence alerts (values 3, 6, 7, 8)
#     high_conf_values = [3, 6, 7, 8]

#     # Create high-confidence mask
#     dist_high_conf = VEGDISTSTATUS.remap(
#         high_conf_values, [1] * len(high_conf_values), 0
#     )

#     return dist_high_conf.updateMask(jrc_gfc_2020_prep()).rename(
#         "DIST_after_2020"
#     )  # Mask alerts to forest and rename band


# TMF_deg_before_2020
def tmf_deg_before_2020_prep():
    tmf_deg = ee.ImageCollection("projects/JRC/TMF/v1_2023/DegradationYear").mosaic()
    return (tmf_deg.lte(2020)).And(tmf_deg.gte(2000)).rename("TMF_deg_before_2020")


# TMF_deg_after_2020
def tmf_deg_after_2020_prep():
    tmf_deg = ee.ImageCollection("projects/JRC/TMF/v1_2023/DegradationYear").mosaic()
    return tmf_deg.gt(2020).rename("TMF_deg_after_2020")


# tmf_def_before_2020
def tmf_def_before_2020_prep():
    tmf_def = ee.ImageCollection("projects/JRC/TMF/v1_2023/DeforestationYear").mosaic()
    return (tmf_def.lte(2020)).And(tmf_def.gte(2000)).rename("TMF_def_before_2020")


# tmf_def_after_2020
def tmf_def_after_2020_prep():
    tmf_def = ee.ImageCollection("projects/JRC/TMF/v1_2023/DeforestationYear").mosaic()
    return tmf_def.gt(2020).rename("TMF_def_after_2020")


# GFC_loss_before_2020 (loss within 10 percent cover; includes 2020; correct for version 11)
def glad_gfc_loss_before_2020_prep():
    # Load the Global Forest Change dataset
    gfc = ee.Image("UMD/hansen/global_forest_change_2023_v1_11")
    gfc_loss = (
        gfc.select(["lossyear"]).lte(20).And(gfc.select(["treecover2000"]).gt(10))
    )
    return gfc_loss.rename("GFC_loss_before_2020")


# GFC_loss_after_2020 (loss within 10 percent cover; correct for version 11)
def glad_gfc_loss_after_2020_prep():
    # Load the Global Forest Change dataset
    gfc = ee.Image("UMD/hansen/global_forest_change_2023_v1_11")
    gfc_loss = gfc.select(["lossyear"]).gt(20).And(gfc.select(["treecover2000"]).gt(10))
    return gfc_loss.rename("GFC_loss_after_2020")


# MODIS_fire_before_2020
def modis_fire_before_2020_prep():
    modis_fire = ee.ImageCollection("MODIS/061/MCD64A1")
    start_year = 2000
    end_year = 2020
    date_st = str(start_year) + "-01-01"
    date_ed = str(end_year) + "-12-31"
    return (
        modis_fire.filterDate(date_st, date_ed)
        .mosaic()
        .select(["BurnDate"])
        .gte(0)
        .rename("MODIS_fire_before_2020")
    )


# MODIS_fire_after_2020
def modis_fire_after_2020_prep():
    modis_fire = ee.ImageCollection("MODIS/061/MCD64A1")
    start_year = 2021
    end_year = datetime.now().year
    date_st = str(start_year) + "-01-01"
    date_ed = str(end_year) + "-12-31"
    return (
        modis_fire.filterDate(date_st, date_ed)
        .mosaic()
        .select(["BurnDate"])
        .gte(0)
        .rename("MODIS_fire_after_2020")
    )


# ESA_fire_before_2020
def esa_fire_before_2020_prep():
    esa_fire = ee.ImageCollection("ESA/CCI/FireCCI/5_1")
    start_year = 2000
    end_year = 2020
    date_st = str(start_year) + "-01-01"
    date_ed = str(end_year) + "-12-31"
    return (
        esa_fire.filterDate(date_st, date_ed)
        .mosaic()
        .select(["BurnDate"])
        .gte(0)
        .rename("ESA_fire_before_2020")
    )


#########################logging concessions
# http://data.globalforestwatch.org/datasets?q=logging&sort_by=relevance
def logging_concessions_prep():
    RCA = ee.FeatureCollection(
        "projects/ee-whisp/assets/logging/RCA_Permis_dExploitation_et_dAmenagement"
    )
    RCA_binary = ee.Image().paint(RCA, 1)
    CMR = ee.FeatureCollection(
        "projects/ee-whisp/assets/logging/Cameroon_Forest_Management_Units"
    )
    CMR_binary = ee.Image().paint(CMR, 1)
    Eq_G = ee.FeatureCollection(
        "projects/ee-whisp/assets/logging/Equatorial_Guinea_logging_concessions"
    )
    Eq_G_binary = ee.Image().paint(Eq_G, 1)
    DRC = ee.FeatureCollection(
        "projects/ee-whisp/assets/logging/DRC_Forest_concession_agreements"
    )
    DRC_binary = ee.Image().paint(DRC, 1)
    Liberia = ee.FeatureCollection(
        "projects/ee-whisp/assets/logging/Liberia_Forest_Management_Contracts"
    )
    Liberia_binary = ee.Image().paint(Liberia, 1)
    RoC = ee.FeatureCollection(
        "projects/ee-whisp/assets/logging/Republic_of_the_Congo_logging_concessions"
    )
    Roc_binary = ee.Image().paint(RoC, 1)
    Sarawak = ee.FeatureCollection(
        "projects/ee-whisp/assets/logging/Sarawak_logging_concessions"
    )
    Sarawak_binary = ee.Image().paint(Sarawak, 1)
    logging_concessions_binary = ee.ImageCollection(
        [
            RCA_binary,
            CMR_binary,
            Eq_G_binary,
            DRC_binary,
            Liberia_binary,
            Roc_binary,
            Sarawak_binary,
        ]
    ).mosaic()

    return logging_concessions_binary.unmask().rename("GFW_logging")


###Combining datasets

# def combine_datasets():
#     """Combines datasets into a single multiband image, with fallback if assets are missing."""
#     img_combined = ee.Image(1).rename(geometry_area_column)

#     # Combine images directly
#     for img in [func() for func in list_functions()]:
#         try:
#             img_combined = img_combined.addBands(img)
#         except ee.EEException as e:
#             # logger.error(f"Error adding image: {e}")
#             print(f"Error adding image: {e}")

#     try:
#         # Attempt to print band names to check for errors
#         print(img_combined.bandNames().getInfo())
#     except ee.EEException as e:
#         # logger.error(f"Error printing band names: {e}")
#         # logger.info("Running code for filtering to only valid datasets due to error in input")
#         print("using valid datasets filter due to error in input")
#         # Validate images
#         images_to_test = [func() for func in list_functions()]
#         valid_imgs = keep_valid_images(images_to_test)  # Validate images

#         # Retry combining images after validation
#         img_combined = ee.Image(1).rename(geometry_area_column)
#         for img in valid_imgs:
#             img_combined = img_combined.addBands(img)

#     img_combined = img_combined.multiply(ee.Image.pixelArea())

#     return img_combined


def combine_datasets(pixel_area=True):
    """
    Combines all available Whisp datasets into a single multiband Earth Engine image.

    This function automatically collects all dataset preparation functions (ending with '_prep')
    in the current module and combines their outputs into a single multiband image. It includes
    error handling to manage missing or invalid datasets, and provides two output
    formats based on the pixel_area parameter.

    Args:
        pixel_area (bool, optional): Controls the output format of the combined image:
            - When True (default): Returns pixel values multiplied by their area in square meters.
              This is used for standard Whisp processing where area-weighted values are used
              for accurate statistics calculation.
            - When False: Returns values as int8 (binary 0/1 values) to produce a smaller image
              for visualization or when raw presence/absence is needed without area weighting.

    Returns:
        ee.Image: A multiband Earth Engine image containing all valid datasets as separate bands.
        The first band is always named using the geometry_area_column value (typically "Area").

        When pixel_area=True:
            - Values represent the area in square meters of each dataset within a pixel
            - Useful for statistical analysis and area calculations

        When pixel_area=False:
            - Values are binary (0/1) represented as int8 data type
            - More efficient for visualization and storage
            - Useful when only presence/absence information is needed

    Examples:
        >>> # Get area-weighted image for statistics calculation
        >>> area_weighted_image = combine_datasets()
        >>> # Compute statistics with this image
        >>> stats = ee.Image(area_weighted_image).reduceRegion(...)
        >>>
        >>> # Get binary presence/absence image for visualization
        >>> binary_image = combine_datasets(pixel_area=False)
        >>> # Use for visualization
        >>> Map.addLayer(binary_image.select('GFC_TC_2020'), {'min': 0, 'max': 1}, 'Tree Cover 2020')
    """

    img_combined = ee.Image(1).rename(geometry_area_column)

    # Combine images directly
    for img in [func() for func in list_functions()]:
        try:
            img_combined = img_combined.addBands(img)
        except ee.EEException as e:
            # logger.error(f"Error adding image: {e}")
            print(f"Error adding image: {e}")

    try:
        # Attempt to print band names to check for errors
        print(img_combined.bandNames().getInfo())
    except ee.EEException as e:
        # logger.error(f"Error printing band names: {e}")
        # logger.info("Running code for filtering to only valid datasets due to error in input")
        print("using valid datasets filter due to error in input")
        # Validate images
        images_to_test = [func() for func in list_functions()]
        valid_imgs = keep_valid_images(images_to_test)  # Validate images

        # Retry combining images after validation
        img_combined = ee.Image(1).rename(geometry_area_column)
        for img in valid_imgs:
            img_combined = img_combined.addBands(img)

    if pixel_area:
        img_combined = img_combined.multiply(ee.Image.pixelArea())
    else:
        img_combined = img_combined.unmask().int8()

    return img_combined


######helper functions to check images


# list all functions ending with "_prep" (in the current script)
def list_functions():
    # Use the module's globals to get all defined functions
    current_module = inspect.getmodule(inspect.currentframe())
    functions = [
        func
        for name, func in inspect.getmembers(current_module, inspect.isfunction)
        if name.endswith("_prep")
    ]
    return functions


def keep_valid_images(images):
    """Keeps only valid images."""
    valid_images = []
    for img in images:
        try:
            img.getInfo()  # This will raise an exception if the image is invalid
            valid_images.append(img)
        except ee.EEException as e:
            # logger.error(f"Invalid image: {e}")
            print(f"Invalid image: {e}")
    return valid_images


# function to check if an image is valid
def ee_image_checker(image):
    """
    Tests if the input is a valid ee.Image.

    Args:
        image: An ee.Image object.

    Returns:
        bool: True if the input is a valid ee.Image, False otherwise.
    """
    try:
        if ee.Algorithms.ObjectType(image).getInfo() == "Image":
            # Trigger some action on the image to ensure it's a valid image
            image.getInfo()  # This will raise an exception if the image is invalid
            return True
    except ee.EEException as e:
        print(f"Image validation failed with EEException: {e}")
    except Exception as e:
        print(f"Image validation failed with exception: {e}")
    return False


# print(combine_valid_datasets().bandNames().getInfo())
# print(combine_datasets().bandNames().getInfo())
