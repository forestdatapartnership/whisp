from parameters.config_lookups import lookup_gee_datasets

def make_processing_lists_from_gee_datasets_lookup(lookup_gee_datasets):
    # global all_dataset_list
    global buffer_stats_list
    global presence_only_flag_list
    global country_allocation_stats_only_list
    global plot_stats_list
    global decimal_place_column_list
    
    all_dataset_list = list(lookup_gee_datasets["dataset_name"])
    
    buffer_stats_list = list(lookup_gee_datasets["dataset_name"][(lookup_gee_datasets["local_buffer"]==1)])
    
    presence_only_flag_list = list(lookup_gee_datasets["dataset_name"][(lookup_gee_datasets["presence_only_flag"]==1)])
    
    country_allocation_stats_only_list = list(lookup_gee_datasets["dataset_name"]     
                                              [(lookup_gee_datasets["country_allocation_stats_only"]==1)])
    
    # for regular stat calculation in ploygon 
    # remove country stats (as modal only) and remove buffer stats (assuming buffer only - NB this may change)
    plot_stats_list = [i for i in all_dataset_list if i not in 
                              country_allocation_stats_only_list+buffer_stats_list] 
    
    decimal_place_column_list =  [i for i in all_dataset_list if i not in presence_only_flag_list + country_allocation_stats_only_list]
    
    return 
    local_buffer_stats_list, 
    presence_only_flag_list, 
    country_allocation_stats_only_list, 
    plot_stats_list, 
    decimal_place_column_list

make_processing_lists_from_gee_datasets_lookup(lookup_gee_datasets)
