
#define buffer distance (m)
local_alerts_buffer_radius = 2000 

# Define how many days back for alerts
how_many_days_back = -(365*2)  # must be negative

### alternative is to use day after EUDR
## i.e., using 01/01/2021 and the start date
# start_date_yyDDD=21001 

#max error in buffer (coarser buffers are quicker)
max_error_alert_buff = 100
