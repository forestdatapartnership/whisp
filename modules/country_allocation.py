import ee
import geemap

from parameters.config_lookups import lookup_gee_datasets
import modules.area_stats as area_stats

def find_country_from_modal_stats(
    roi,
    image_collection,
    reducer_choice,
    geo_id_column,
    country_dataset_id,
    admin_code_col_name,
    lookup_country_codes_to_names):
    
    """Makes on-the-fly look up table to link country name/iso3 to geo id based on raster stats (uses rasterised admin layer with admin codes as pixel values)"""

    #for each geo id finds most common value in that geometry (i.e. "mode" statistic)
    zonal_stats_country_codes = area_stats.zonal_stats_iCol(roi,
                                      image_collection.filter(ee.Filter.eq("country_allocation_stats_only",1)),
                                      reducer_choice)# all but alerts
    

    df_stats_country_codes = geemap.ee_to_pandas(zonal_stats_country_codes) #NB limit of 5000 (i have code if this happens)

    #get dataset name from lookup to use to select

    dataset_name =  list(lookup_gee_datasets["dataset_name"]     
                                              [(lookup_gee_datasets["dataset_id"]==country_dataset_id)])[0]

    #get modal stats for dataset
    lookup_geo_id_to_country_codes = df_stats_country_codes[df_stats_country_codes["dataset_name"]==dataset_name] 
    
    #choose only columns needed
    lookup_geo_id_to_country_codes = lookup_geo_id_to_country_codes[[geo_id_column, 'mode']]
    
    # change names for a clean join 
    lookup_geo_id_to_country_codes = lookup_geo_id_to_country_codes.rename(columns={"mode":admin_code_col_name})
    
    lookup_geo_id_to_country_names = lookup_geo_id_to_country_codes.merge(
        lookup_country_codes_to_names,on=admin_code_col_name,how="inner").drop(admin_code_col_name,axis=1) # join geo id to the lookup_table countaining "Country_names"

# lookup_geo_id_to_ISO3 = lookup_geo_id_to_GAUL_codes.merge(lookup_country_codes_to_ISO3,on="ADM0_CODE",how="inner").drop("ADM0_CODE",axis=1) # join geo id to the GAUL_lookup_table countaining "Country_names"

    return lookup_geo_id_to_country_names