***** list_hypercat_streams.csv format: *****

[core_url],[feed_type*],[feed id],[request_type_1**],[datastream ID],[request_type_2***],[params list (Dict)],[user's feed name]

* feed_type options:
    events
    locations
    sensors
    sensors2
    journeys

** request_type_1:
    none
    datastreams
    features

*** request_type_2:
    none
    datapoints
    events


***** Eg. *****

http://api.bt-hypercat.com,sensors,86a25d4e-25fc-4ebf-a00d-0a603858c7e1,datastreams,0,datapoints,{'limit': '100'},Anns_carpark_stream


***** Sample parsing code: *****

list_params = line.split(",")
core_url_string = list_params[0]
feed_type = Feed_type[list_params[1]]
feed_id = list_params[2]
request_type_1 = Request_type_1[list_params[3]]
datastream_id = int(list_params[4])
request_type_2 = Request_type_2[list_params[5]]
params_list_str = list_params[6]  # {'limit': '100'}
users_feed_name = list_params[7].rstrip('\n')
