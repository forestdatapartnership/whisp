
def reproject_to_template(rasterised_vector,template_image):
    from modules.area_stats import get_scale_from_image
    """takes an image that has been rasterised but without a scale (resolution) and reprojects to template image CRS and resolution"""
    #reproject an image
    crs_template = template_image.select(0).projection().crs().getInfo()

    output_image = rasterised_vector.reproject(
      crs= crs_template,
      scale= get_scale_from_image(template_image),
    ).int8()
    
    return output_image