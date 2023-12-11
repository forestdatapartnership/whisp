### parameters for random feature creation - useful for testing functionality in different parts of the world and with many features

#if true will make number_of_points per admin boundary (i.e. no of points x no of boundaries), as opposed tob spread across them
points_per_admin_boundary = False 

#how many random admin boundaries do we select
number_of_boundaries = 40

#how many points  
number_of_points = 40

#buffer
buffer_distance_meters = 500

#max error in m (coarse is quicker to run) 
max_error_alert_buff = 100 

seed = 18 # so it can be reproducable - each number is a new random combination                                                                                                
   
max_error = 1000 #in meters for admin boundaries (can be coarse ot speed up computation, as just example)
