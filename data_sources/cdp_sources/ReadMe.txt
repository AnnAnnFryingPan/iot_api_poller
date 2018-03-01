NOTE: CDP is more complicated due to time series type data not being recorded in as a time series!! Have to get higher level (eg "https://api.cityverve.org.uk/v1/entity/crime" and then get all children to get the required info. To get the children, we need a path to the time field from each child (eg "entity.occurred"  -->  child_json['entity']['occurred'] == the required time).

Each input lines can be JSON:
{"feed_info": {"href": "https://api.cityverve.org.uk/v1/entity/crime", "time_field":"entity.occurred"}, "user_defined_name": "crimes_by_day_occurred", "stream_params": ["https://api.cityverve.org.uk","v1", "entity","crime","","static","","datapoints", "{}"]}

Can be csv:
"https://api.cityverve.org.uk","v1", "entity","crime","","static","","datapoints", "{}","entity.occurred","crimes_by_day_occurred"




