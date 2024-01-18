import os
import ee

# dataset_id = 10


def creaf_descals_palm_prep(dataset_id):
    import modules.area_stats as area_stats
    # Import the dataset; a collection of composite granules from 2019.
    oil_palm_descals_raw = ee.ImageCollection('BIOPAMA/GlobalOilPalm/v1');

    # Select the classification band and mosaic all of the granules into a single image.
    oil_palm_descals_mosaic = oil_palm_descals_raw.select('classification').mosaic();

    # Visualisation only - not needed: create a mask to add transparency to non-oil palm plantation class pixels.
    mask = oil_palm_descals_mosaic.neq(3);

    mask = mask.where(mask.eq(0), 0.6); #not sure about this - from online (check)

    oil_palm_descals_binary = oil_palm_descals_mosaic.lte(2) #choosing to ignore mask 

    oil_palm_descals_binary = area_stats.set_scale_property_from_image(
        oil_palm_descals_binary,oil_palm_descals_raw.first(),0,debug=True
    )
    
    output_image = oil_palm_descals_binary
    
    return output_image.set("dataset_id",dataset_id)