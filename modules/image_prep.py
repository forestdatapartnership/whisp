import ee
import pandas as pd

def remap_image_from_csv_cols (image,csv_path,from_col,to_col,default_value):
    df =pd.read_csv(csv_path)
    image_out= remap_image_from_dataframe_cols(image,df,from_col,to_col,default_value)
    return image_out

def remap_image_from_dataframe_cols (image,df,from_col,to_col,default_value):
    from_list= df[from_col].values.tolist()
    to_list= df[to_col].values.tolist()
    image_out = (image.remap(from_list,to_list,default_value))
    return image_out
