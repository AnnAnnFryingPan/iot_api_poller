import time
from tendo import singleton
import sys
import datetime
import os

sys.path.insert(0, '../data_hub_call')
sys.path.insert(0, '../influxdb_connection')

import influxdb_connection
from data_stream_fetch_list import Data_stream_fetch_list
from data_hub_call_bt import Data_hub_call_bt
from data_hub_call_osisoft_pi import Data_hub_call_osisoft_pi

me = singleton.SingleInstance() # will sys.exit(-1) if other instance is running

hypercat_sources_dir = "hypercat_sources"
hypercat_credentials_file = 'hypercat_credentials.csv'
hypercat_streams_file = 'list_hypercat_streams.csv'
osisoft_pi_sources_dir = 'osisoft_pi_sources'
osisoft_pi_streams_file = 'list_osisoft-pi_streams.csv'


def main():
    try:
        if (len(sys.argv) > 0):
            streams_dir = sys.argv[1]
            influxdb_db_name = sys.argv[2]
            polling_interval = float(sys.argv[3])
    except:
        print('Error reading command line argument')
        raise

    running = True
    while running:
        start = time.clock()

        api_streams = Data_stream_fetch_list()

        with open(os.path.join(streams_dir, hypercat_sources_dir, hypercat_credentials_file)) as f_creds:
            credentials_file = f_creds.readlines()
            list_params = credentials_file[0].split(",")
            hypercat_api_key = list_params[0]
            hypercat_username = list_params[1].rstrip('\n')
        with open(os.path.join(streams_dir, hypercat_sources_dir, hypercat_streams_file)) as f_streams:
            api_streams_file = f_streams.readlines()
        api_streams.append_hypercat_stream_list(hypercat_username, hypercat_api_key, api_streams_file)

        with open(os.path.join(streams_dir, osisoft_pi_sources_dir, osisoft_pi_streams_file)) as f_streams:
            api_streams_file = f_streams.readlines()
        api_streams.append_pi_stream_list(api_streams_file)


        print("No. of streams to be processed: " + str(len(api_streams)))
        print(datetime.datetime.now())

        for stream in api_streams.streams:

            # poll API
            if(stream.api_core_url == Data_hub_call_bt.core_URL):
                try:
                    bt_hub = Data_hub_call_bt(stream)
                    bt_hub_response = bt_hub.call_api_fetch(stream.params)
                    print("BT-Hub call successful. " + str(len(bt_hub_response)) + " returned rows from " + stream.users_feed_name)
                except:
                    print("Error calling Hub: " + stream.api_core_url)
                try:
                    influx_db_connection = influxdb_connection.Influxdb_connection(influxdb_db_name)
                    influx_db_connection.import_hypercat_response_json(bt_hub_response.decode("utf-8"),
                                                                       stream.users_feed_name)
                    print("influxDb call successful: To " + stream.users_feed_name)
                    # print(influx_db_connection.query_database('select value from ' + stream.users_feed_name + ';'))
                except:
                    print("Error populating influx-db")

            elif(stream.api_core_url == Data_hub_call_osisoft_pi.core_URL):
                try:
                    pi_hub = Data_hub_call_osisoft_pi(stream)
                    pi_hub_response = pi_hub.call_api_fetch(stream.params)
                    print("Pi-Hub call successful. " + str(
                        len(pi_hub_response)) + " returned rows from " + stream.users_feed_name)
                    #print(pi_hub_response)
                except:
                    print("Error calling Pi hub: " + stream.api_core_url)

                try:
                    influx_db_connection = influxdb_connection.Influxdb_connection(influxdb_db_name)
                    influx_db_connection.import_pi_response_json(pi_hub_response.decode("utf-8"),
                                                                       stream.users_feed_name)
                    print("influxDb call successful: To " + stream.users_feed_name)
                    # print(influx_db_connection.query_database('select value from ' + stream.users_feed_name + ';'))
                except:
                    print("Error populating influx-db")



        work_duration = time.clock() - start
        time.sleep(polling_interval - work_duration)


if __name__ == '__main__':
  main()