#currently storing code here - not properly functional

import os
import ee

ee.Initialize()

from parameters.config_output_naming import targetImageCollId
from parameters.config_runtime import * # make explicit

if export_image_collection_to_asset:

    exportRegion = ee.FeatureCollection("FAO/GAUL_SIMPLIFIED_500m/2015/level0").filter(
        ee.Filter.inList("ADM0_NAME",["Côte d'Ivoire", "Indonesia","Malaysia","Ghana"])).geometry()

    if make_empty_image_coll == True:
        try:
            getAssetInfo = ee.data.getAsset(targetImageCollId)
            if debug: print ("Target image collection exists: ",targetImageCollId)
        except:
            ee.data.createAsset({'type': 'ImageCollection'}, targetImageCollId)#make a new image collection
            print ("New (empty) image collection created: ",targetImageCollId)
            skip_export_if_asset_exists = True# as it sounds like. Saves possibility of lots of red errors in Tasks list in code editor

        def imageNames (imageCollection):##list existing images in collection (if any)
            return imageCollection.aggregate_array("system:id").getInfo()

    imageCollectionImageList = (imageNames(ee.ImageCollection(targetImageCollId)))


    for i in range(images_IC.size().getInfo()):

        image_new = ee.Image(images_IC.toList(100,0).get(i))

        dataset_name = image_new.get("system:index").getInfo()

        output_scale = image_new.get("scale").getInfo()

        out_name = targetImageCollId+"/"+dataset_name

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