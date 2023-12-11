### general runtime parameters

use_existing_image_collection = False  # use existing image collection (faster if exists), or create one on the fly in GEE (takes longer) (True or False)

debug = True  # get print messages or not (e.g. for debugging code etc) (True or False)


## export to image collection asset

export_image_collection_to_asset =False  # choose to export datasets to an image collection asset (makes faster data loading times). Set to True or False.

make_empty_image_coll = True # if true then code will add an empty image collection (see parmaters.output_naming), if one doesn't exist already. Set to True or False.

skipExportIfAssetExists = True # if image with same name exists in image collection avoid exporting. Set to True or False.
