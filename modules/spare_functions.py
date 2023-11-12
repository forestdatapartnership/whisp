## slows things down (unless use client side maybe)
# if ee.Algorithms.ObjectType(roi).getInfo() == "FeatureCollection": 
#     fc_stats_combined = reduceStatsIC(roi,images_IC)
# else:
#     fc_stats_combined = reduceStatsIC(ee.FeatureCollection([roi]),images_IC)    

#fc_stats_combined


# def add_area_km2_property_to_feature_collection(fc,geometry_area_column):
#     roi = roi.map(add_area_km2_property_to_feature)

# def add_area_km2_property_to_feature_collection(fc,geometry_area_column):
#     def add_area_km2_property_to_feature (feature):
#         feature = feature.set(geometry_area_column,feature.area().divide(1e6))#add area
#         return feature
#     outFC = fc.map(add_area_km2_property_to_feature)
#     return outFC


def reduceStatsIC (featureCollection,imageCollection,reducer_choice):
    "Calculating summary statistics for an image where it falls in a feature's bounds. This is then repoeated for others in an image collection""" 
    roi = featureCollection
    def reduceStats (image):
        scale=image.get("scale")

        fc = ee.FeatureCollection(image.reduceRegions(collection=roi,
                                                      reducer=reducer_choice,
                                                      scale=scale))

        fc = fc.map(lambda feature: feature.set("dataset_name",image.get("system:index")))
        return fc
    fc_out = imageCollection.map(reduceStats).flatten()
    return fc_out



##.map(lambda image: image.clip(roi_alerts_buffer.bounds()))

# df_combined["test"]=np.where((df_combined["sum"]>0) & (df_combined["dataset_name"].str.contains("overlap")),"True","False")
# df_combined 
# df_combined.to_csv(path_or_buf=out_file_long,header=True,index=False)
