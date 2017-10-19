***** list_osisoft-pi_requests.csv format: *****

[core api url], [data type*], [params], [request type**], [user's feed name],
	[feed_info (Optional, json dict): longitude (float), latitude (float), tagNames (string delimited by |), unitText (string)]

*Data type options:
    streams = 1
    streamsets = 2
    elements = 3
    attributes = 4

**Request type options:     
    none = 0
    plot = 1
    interpolated = 2
    summary = 3
    recorded = 4


***** Eg. *****

https://130.88.97.137/piwebapi,streamsets,E0XpbRmwnc7kq0OSy1LydJJQxey0y1BT5hGA3gBQVqtCQgVk0tUEktUDAxLkRTLk1BTi5BQy5VS1xUUklBTkdVTFVNXEFJUiBRVUFMSVRZXE9YRk9SRCBST0FE,plot,Anns_Pi_feed_Nitrogen
https://130.88.97.137/piwebapi,streams,A0EXpbRmwnc7kq0OSy1LydJJQvX1z10I55hGA3QBQVqtCQgXVe0MsMt-km_jVETA_vL0QVk0tUEktUDAxLkRTLk1BTi5BQy5VS1xUUklBTkdVTFVNXFdFQVRIRVJ8Q0xPVUQgQkFTRQ,recorded,Triangulum_cloudbase, {"longitude":96, "latitude":97.675, "tagNames":"cloudbase|weather", "unitText":"cloudbase"}
