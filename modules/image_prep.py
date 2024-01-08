import ee
import pandas as pd


def add_lookup_property_to_image_collection(image_collection, collection_join_column, 
                                            lookup_dataframe, df_join_column, 
                                            df_column_to_add, new_property_name):
    
    """adds property from lookup table using common column/property Note: one-to-one joins only"""
    all_images_list = image_collection.aggregate_array(collection_join_column).getInfo() #to loop over
    
    new_list=[] #make empty list to fill with images
    
    for i in all_images_list: 
        
        #get value to add to the property from data frame using indexes
        new_value = (lookup_dataframe[df_column_to_add][lookup_dataframe[df_join_column]==i]).item()
        
        #filter to jsut image with this property - must be unique else will get errors
        image = image_collection.filter(ee.Filter.eq(collection_join_column,i)).first()
        
        #set new property
        image_w_new_property = image.set(new_property_name,new_value)
        
        #append image with new property to list
        new_list = ee.List(new_list).add(ee.List(image_w_new_property))
        
    return ee.ImageCollection(new_list) #turn list into output image collection


def remap_image_from_csv_cols (image,csv_path,from_col,to_col,default_value):
    df =pd.read_csv(csv_path)
    image_out= remap_image_from_dataframe_cols(image,df,from_col,to_col,default_value)
    return image_out


def remap_image_from_dataframe_cols (image,df,from_col,to_col,default_value):
    from_list= df[from_col].values.tolist()
    to_list= df[to_col].values.tolist()
    image_out = (image.remap(from_list,to_list,default_value))
    return image_out


#make into a function
def export_image_collection_to_asset(make_empty_image_coll, image_col_to_export, target_image_col_id, exportRegion, skip_export_if_asset_exists, asset_exists_property="system:index", debug=True):

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
    