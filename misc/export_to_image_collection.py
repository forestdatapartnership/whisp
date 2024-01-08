#currently storing code here - not properly functional

import os
import ee

ee.Initialize()

from parameters.config_output_naming import target_image_col_id
from parameters.config_runtime import * # make explicit

#make into a function
def export_image_collection_to_asset(make_empty_image_coll, image_collection, target_image_col_id, exportRegion, skip_export_if_asset_exists, asset_exists_property="system:index", debug=True):

    if make_empty_image_coll == True:
        try:
            getAssetInfo = ee.data.getAsset(target_image_col_id)
            if debug: print ("Target image collection exists: ",target_image_col_id)
        except:
            ee.data.createAsset({'type': 'ImageCollection'}, target_image_col_id)#make a new image collection
            print ("New (empty) image collection created: ",target_image_col_id)
            skip_export_if_asset_exists = True# as it sounds like. Saves possibility of lots of red errors in Tasks list in code editor

        def imageNames (imageCollection):##list existing images in collection (if any)
            return imageCollection.aggregate_array(asset_exists_property).getInfo()

    imageCollectionImageList = (imageNames(ee.ImageCollection(target_image_col_id)))


    for i in range(image_col_to_export.size().getInfo()):

        image_new = ee.Image(image_col_to_export.toList(10000,0).get(i))

        dataset_name = image_new.get(asset_exists_property).getInfo()

        output_scale = image_new.get("scale").getInfo()

        out_name = target_image_col_id+"/"+dataset_name

        task = ee.batch.Export.image.toAsset(image= image_new,\
                                         description= dataset_name,\
                                         assetId=out_name,\
                                         scale= output_scale,\
                                         maxPixels=1e13,\
                                         region=exportRegion)

        if ((skip_export_if_asset_exists==True) and (out_name in imageCollectionImageList)):
            if debug: print ("testing - not exporting NB asset exists")
        else:
            task.start()###code out if testing and dont want to export assets

            if debug: print ("exporting image: "+ out_name)


if debug: print ("finished")
    