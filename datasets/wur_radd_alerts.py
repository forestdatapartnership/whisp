import os
import ee

# import modules.image_prep as image_prep
# import modules.area_stats as area_stats

# from datetime import datetime
# from parameters.config_radd_alerts import how_many_days_back

ee.Initialize()

dataset_id = 8

def wur_radd_alerts_prep(dataset_id):
    from datetime import datetime
    import modules.area_stats as area_stats
    
    from parameters.config_radd_alerts import how_many_days_back

    # Getting today's date
    ee_now =ee.Date(datetime.now())#.format()

    # Calculate the start date
    start_date = ee_now.advance(how_many_days_back, "day")

    # Needs to be in yyDDD format and needs to be a number, so need to parse too
    start_date_yyDDD = ee.Number.parse(start_date.format('yyDDD'))

    # Define the Image Collection
    radd = ee.ImageCollection('projects/radar-wur/raddalert/v1')

    # Forest baseline (from Primart HT forests data)
    forest_baseline = ee.Image(radd.filterMetadata('layer', 'contains', 'forest_baseline')
        .mosaic())

    # Latest RADD alert
    latest_radd_alert = ee.Image(radd.filterMetadata('layer', 'contains', 'alert')
        .sort('system:time_end', False)
        .mosaic())

    # Update mask for RADD alert to be within primary forest (TO CHECK maybe unnecessry step)
    latest_radd_alert_masked = latest_radd_alert.select('Alert')#.updateMask(forest_baseline)

    # Mask confirmed alerts #TO CHECK do we want to be more conservative?
    latest_radd_alert_masked_confirmed = latest_radd_alert_masked.unmask().eq(3)

    # Update mask for confirmed alerts by date
    latest_radd_alert_confirmed_recent = latest_radd_alert.select('Date').gte(start_date_yyDDD).selfMask()
    latest_radd_alert_confirmed_recent

    latest_radd_alert_confirmed_recent = area_stats.set_scale_property_from_image(
        latest_radd_alert_confirmed_recent,radd.first(),debug=True)

    output_image = latest_radd_alert_confirmed_recent
    
    return output_image.set("dataset_id",dataset_id)