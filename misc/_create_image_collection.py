#currently storing code here - not properly functional

import os
import ee

ee.Initialize()

export_image_collection_to_asset =True # NB shift to parameters if code is kept in this script


if export_image_collection_to_asset:

    targetImageCollId = "users/andyarnell10/fdap_dpi/imageCol_trial_2"

    createNewImageColl = True # if true then code will add outputImageColl if it doesn't exist already. Code at end of cell/section.

    skipExportIfAssetExists = True # if image with the same name exists avoid exporting

    exportRegion = ee.FeatureCollection("FAO/GAUL_SIMPLIFIED_500m/2015/level0").filter(
        ee.Filter.inList("ADM0_NAME",["Côte d'Ivoire", "Indonesia","Malaysia","Ghana"])).geometry()


    if createNewImageColl == True:
        try:
            getAssetInfo = ee.data.getAsset(targetImageCollId)
            if debug: print ("Target image collection exists: ",targetImageCollId)
        except:
            ee.data.createAsset({'type': 'ImageCollection'}, targetImageCollId)#make a new image collection
            print ("New (empty) image collection created: ",targetImageCollId)
            skipExportIfAssetExists = True# as it sounds like. Saves possibility of lots of red errors in Tasks list in code editor

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

        if ((skipExportIfAssetExists==True) and (out_name in imageCollectionImageList)):
            if debug: print ("testing - not exporting NB asset exists")
        else:
            task.start()###code out if testing and dont want to export assets

            if debug: print ("exporting image: "+ out_name)


if debug: print ("finished")