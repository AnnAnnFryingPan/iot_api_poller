from iot_api_poller.iotApiPoller import IotApiPoller
poller = IotApiPoller(True, '/home/ann/Code/iot_api_poller/iot_api_poller', file, 3600.0, True)
poller.start()

