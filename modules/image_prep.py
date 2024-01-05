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

