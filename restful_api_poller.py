import os
import sys
import time

sys.path.insert(0, '../data_hub_call')
sys.path.insert(0, '../influxdb_connection')

from request_info_fetch_list import Request_info_fetch_list
from data_hub_call_restful_bt import Data_hub_call_restful_bt
from data_hub_call_osisoft_pi import Data_hub_call_osisoft_pi


try:
    import influxdb_connection
    force_file = False
except ImportError:
    force_file = True


sys.path.insert(0, '../data_hub_call')

RESTFUL_BT_SOURCES_DIR = "restful_bt_sources"
BT_HUB_CREDENTIALS_FILE = 'bt_hub_credentials.csv'
RESTFUL_BT_REQUESTS_FILE = 'list_restful_bt_requests.csv'
TRIANGULUM_SOURCES_DIR = 'osisoft_pi_sources'
TRIANGULUM_REQUESTS_FILE = 'list_osisoft-pi_requests.csv'
INPUT_DIR = 'data_sources'
CSV_OUTPUT_DIR = 'output'
CSV_OUTPUT_FILE_PREFIX_BT = 'bt_'
CSV_OUTPUT_FILE_PREFIX_TRI = 'triangulum_'


class Restful_api_poller(object):

    def __init__(self, home_dir, influxdb_db_name, polling_interval, get_latest=True):
        self.requests_dir = os.path.join(home_dir, INPUT_DIR)
        self.output_dir = os.path.join(home_dir, CSV_OUTPUT_DIR)
        self.running = False

        if (force_file):
            self.influxdb_db_name = 'file'
        else:
            self.influxdb_db_name = influxdb_db_name
        self.polling_interval = polling_interval
        self.get_latest = get_latest

        self.api_requests = None



    def start(self):
        self.api_requests = Request_info_fetch_list()

        try:
            with open(os.path.join(self.requests_dir, RESTFUL_BT_SOURCES_DIR, BT_HUB_CREDENTIALS_FILE)) as f_creds:
                credentials_file = f_creds.readlines()
                list_params = credentials_file[0].split(",")
                restful_bt_api_key = list_params[0]
                restful_bt_username = list_params[1].rstrip('\n')
        except Exception as err:
            print('Unable to read BT credentials file')
        else:
            try:
                with open(os.path.join(self.requests_dir, RESTFUL_BT_SOURCES_DIR, RESTFUL_BT_REQUESTS_FILE)) as f_requests:
                    api_requests_file = f_requests.readlines()
                    self.api_requests.append_restful_bt_request_list(restful_bt_username, restful_bt_api_key, api_requests_file)
            except Exception as err:
                print(
                    'Unable to read BT streams file')

        try:
            with open(os.path.join(self.requests_dir, TRIANGULUM_SOURCES_DIR, TRIANGULUM_REQUESTS_FILE)) as f_requests:
                api_requests_file = f_requests.readlines()
                self.api_requests.append_pi_request_list(api_requests_file)
        except Exception as err:
            print('Unable to read Triangulum streams file ')

        if (len(self.api_requests) == 0):
            print('Unable to read any streams data from input home directory. Exiting.')
            sys.exit(0)

        influx_db = None

        self.running = True
        while self.running:
            start = time.clock()

            print("")
            print("***** No. of streams to be processed: " + str(len(self.api_requests)) + " *****")

            if (str(self.influxdb_db_name).strip().lower() != 'file'):
                influx_db = influxdb_connection.Influxdb_connection(self.influxdb_db_name)

            for request in self.api_requests.requests:

                # poll API
                if (request.api_core_url == Data_hub_call_restful_bt.core_URL):
                    try:
                        bt_hub = Data_hub_call_restful_bt(request)
                        cur_params = request.params
                        bt_hub_response = bt_hub.call_api_fetch(cur_params, get_latest_only=self.get_latest)
                        print('BT hub response: ' + str(bt_hub_response))
                        if (bt_hub_response['ok']):
                            print("BT-Hub call successful. " + str(
                                str(bt_hub_response[
                                        'returned_matches'])) + " returned rows from " + request.users_feed_name)
                            # print(bt_hub_response['content'])

                            if (influx_db != None):
                                try:
                                    # longitude=None, latitude=None, tagNames=[], unitText=''
                                    influx_db.import_hypercat_response_json(
                                        bt_hub_response['content'],
                                        request.users_feed_name,
                                        request.feed_info)
                                    print("influxDb call successful: To table: " +
                                          request.users_feed_name + " in " + self.influxdb_db_name)
                                except Exception as err:
                                    print("Error populating influx-db: " + str(err))
                                else:
                                    try:
                                        print('select * from ' + request.users_feed_name + ": " +
                                              str(influx_db.query_database(
                                                  'select * from ' + request.users_feed_name + ';')))
                                    except Exception as err:
                                        print("Error reading new data from influx-db.")

                            else:
                                file_spec = os.path.join(self.output_dir,
                                                         CSV_OUTPUT_FILE_PREFIX_BT + request.users_feed_name + '.json')
                                try:
                                    with open(file_spec, 'a+') as csv_file:
                                        csv_file.write(bt_hub_response['content'] + '\n')
                                        # json.dump(bt_hub_response, csv_file)
                                    print("csv file write successful: To file: " + file_spec)
                                except Exception as err:
                                    print('Unable to write to output file: ' + str(err))

                        else:
                            print("Error: call to hub: " + request.api_core_url + " failed with status code: " + \
                                  bt_hub_response['reason'])

                    except Exception as err:
                        print("Error calling Hub: " + request.api_core_url + ". " + str(err))



                elif (request.api_core_url == Data_hub_call_osisoft_pi.core_URL):
                    try:
                        pi_hub = Data_hub_call_osisoft_pi(request)
                        pi_hub_response = pi_hub.call_api_fetch(self.get_latest)
                        print('Pi hub response: ' + str(pi_hub_response))
                        if (pi_hub_response['ok']):
                            print("Pi-Hub call successful. " + str(
                                str(pi_hub_response[
                                        'returned_matches'])) + " returned rows from " + request.users_feed_name)

                            if (influx_db != None):
                                try:
                                    influx_db.import_pi_response_json(
                                        pi_hub_response['content'],
                                        request.users_feed_name,
                                        request.feed_info)
                                    print("influxDb call successful: To table: " +
                                          request.users_feed_name + " in " + self.influxdb_db_name)
                                except Exception as err:
                                    print("Error populating influx-db: " + str(err))
                                else:
                                    try:
                                        print('select * from ' + request.users_feed_name + ": " +
                                              str(influx_db.query_database(
                                                  'select * from ' + request.users_feed_name + ';')))
                                    except Exception as err:
                                        print("Error reading new data from influx-db.")

                            else:
                                file_spec = os.path.join(self.output_dir,
                                                         CSV_OUTPUT_FILE_PREFIX_TRI + request.users_feed_name + '.json')
                                try:
                                    with open(file_spec, 'a+') as csv_file:
                                        csv_file.write(pi_hub_response['content'] + '\n')
                                    print("csv file write successful: To file: " + file_spec)
                                except Exception as err:
                                    print('Unable to write to output file .' + str(err))

                        else:
                            print("Error: call to hub: " + request.api_core_url + " failed with status code:  " + \
                                  pi_hub_response['reason'])

                            # print(pi_hub_response)
                    except Exception as err:
                        print("Error calling Pi hub: " + request.api_core_url + ". " + str(err))

            work_duration = time.clock() - start
            time.sleep(self.polling_interval - work_duration)

    def stop(self):
        self.running = False
        self.api_requests = None
