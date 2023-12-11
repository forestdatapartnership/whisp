from parameters.config_lookups import lookup_gee_datasets

def make_processing_lists_from_gee_datasets_lookup(lookup_gee_datasets):
    # global all_dataset_list
    global local_buffer_stats_list
    global flag_list
    global country_allocation_stats_only_list
    global normal_poly_stats_list
    global decimal_place_column_list
    
    all_dataset_list = list(lookup_gee_datasets["dataset_name"])
    
    local_buffer_stats_list = list(lookup_gee_datasets["dataset_name"][(lookup_gee_datasets["local_buffer"]==1)])
    
    flag_list = list(lookup_gee_datasets["dataset_name"][(lookup_gee_datasets["presence_only_flag"]==1)])
    
    country_allocation_stats_only_list = list(lookup_gee_datasets["dataset_name"]     
                                              [(lookup_gee_datasets["country_allocation_stats_only"]==1)])
    
    # for regular stat calculation in ploygon 
    # remove country stats (as modal only) and remove buffer stats (assuming buffer only - NB this may change)
    normal_poly_stats_list = [i for i in all_dataset_list if i not in 
                              country_allocation_stats_only_list+local_buffer_stats_list] 
    
    decimal_place_column_list =  [i for i in all_dataset_list if i not in flag_list + country_allocation_stats_only_list]
    
    return 
    local_buffer_stats_list, 
    flag_list, 
    country_allocation_stats_only_list, 
    normal_poly_stats_list, 
    decimal_place_column_list

make_processing_lists_from_gee_datasets_lookup(lookup_gee_datasets)
