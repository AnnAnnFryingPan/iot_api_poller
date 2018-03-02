import os
import sys

sys.path.insert(0, '../data_hub_call')
sys.path.insert(0, '../influxdb_connection')

from request_info_fetch_list import Request_info_fetch_list
from data_hub_call_restful_bt import Data_hub_call_restful_bt
from data_hub_call_osisoft_pi import Data_hub_call_osisoft_pi
from data_hub_call_restful_cdp import Data_hub_call_restful_cdp
from poller import Poller


try:
    import influxdb_connection
    force_file = False
except ImportError:
    force_file = True


RESTFUL_BT_SOURCES_DIR = "restful_bt_sources"
BT_HUB_CREDENTIALS_FILE = 'bt_hub_credentials.csv'
RESTFUL_BT_REQUESTS_FILE = 'list_restful_bt_requests.csv'
TRIANGULUM_SOURCES_DIR = 'osisoft_pi_sources'
TRIANGULUM_REQUESTS_FILE = 'list_osisoft-pi_requests.csv'
CDP_SOURCES_DIR = "cdp_sources"
CDP_CREDENTIALS_FILE = 'cdp_credentials.csv'
CDP_REQUESTS_FILE = 'list_cdp_requests.csv'
INPUT_DIR = 'data_sources'
CSV_OUTPUT_DIR = 'output'



class Restful_api_poller(Poller):

    def __init__(self, home_dir, influxdb_db_name, polling_interval, get_latest_only=True):
        super(Restful_api_poller, self).__init__(polling_interval, get_latest_only)

        self.requests_dir = os.path.join(home_dir, INPUT_DIR)
        self.output_dir = os.path.join(home_dir, CSV_OUTPUT_DIR)
        if (force_file):
            self.influxdb_db_name = 'file'
        else:
            self.influxdb_db_name = influxdb_db_name

        self.api_requests = Request_info_fetch_list()
        self.influx_db = None

        self.read_files()



    def do_work(self):
        if (len(self.api_requests) == 0):
            print('Unable to read any streams data from input home directory. Exiting.')
            sys.exit(0)

        print("")
        print("***** No. of streams to be processed: " + str(len(self.api_requests)) + " *****")

        if (str(self.influxdb_db_name).strip().lower() != 'file'):
            self.influx_db = influxdb_connection.Influxdb_connection(self.influxdb_db_name)

        for request in self.api_requests.requests:
            # poll API
            try:
                self.poll_hub(request)
            except Exception as err:
                #type(err)(err.message + ': ' + hub_result.reason)
                print("Not able to poll " + request.users_feed_name + ' at this time. Continuing poller. ' + str(err))


    def read_files(self):
        self.api_requests.clear_all()
        try:
            with open(os.path.join(self.requests_dir, RESTFUL_BT_SOURCES_DIR, BT_HUB_CREDENTIALS_FILE)) as f_creds:
                credentials_file = f_creds.readlines()
                list_params = credentials_file[0].split(",")
                restful_bt_api_key = list_params[0]
                restful_bt_username = list_params[1].rstrip('\n')
        except Exception as err:
            print('Unable to read BT credentials file: ' + os.path.join(self.requests_dir, RESTFUL_BT_SOURCES_DIR,
                                                                        BT_HUB_CREDENTIALS_FILE))
        else:
            try:
                with open(os.path.join(self.requests_dir, RESTFUL_BT_SOURCES_DIR,
                                       RESTFUL_BT_REQUESTS_FILE)) as f_requests:
                    api_requests_file = f_requests.readlines()
                    self.api_requests.append_restful_bt_request_list(restful_bt_username, restful_bt_api_key,
                                                                     api_requests_file)
            except Exception as err:
                print('Unable to read BT streams file: ' + os.path.join(self.requests_dir, RESTFUL_BT_SOURCES_DIR,
                                                                        RESTFUL_BT_REQUESTS_FILE) + str(err))

        try:
            with open(os.path.join(self.requests_dir, CDP_SOURCES_DIR, CDP_CREDENTIALS_FILE)) as f_creds:
                credentials_file = f_creds.readlines()
                list_params = credentials_file[0].split(",")
                cdp_api_key = list_params[0].rstrip('\n')
                #cdp_username = list_params[1].rstrip('\n')
        except Exception as err:
            print('Unable to read CDP credentials file: ' + os.path.join(self.requests_dir, CDP_SOURCES_DIR,
                                                                        CDP_CREDENTIALS_FILE))
        else:
            try:
                with open(os.path.join(self.requests_dir, CDP_SOURCES_DIR,
                                       CDP_REQUESTS_FILE)) as f_requests:
                    api_requests_file = f_requests.readlines()
                    self.api_requests.append_restful_cdp_request_list(cdp_api_key,
                                                                     api_requests_file)
            except Exception as err:
                print('Unable to read CDP streams file: ' + os.path.join(self.requests_dir, CDP_SOURCES_DIR,
                                                                        CDP_REQUESTS_FILE) + str(err))


        try:
            with open(os.path.join(self.requests_dir, TRIANGULUM_SOURCES_DIR,
                                   TRIANGULUM_REQUESTS_FILE)) as f_requests:
                api_requests_file = f_requests.readlines()
                self.api_requests.append_pi_request_list(api_requests_file)
        except Exception as err:
            print('Unable to read Triangulum streams file ')


    def poll_hub(self, request):
        hub = None
        short_name = None
        hub_response = None

        try:
            if request.api_core_url == Data_hub_call_restful_bt.core_URL:
                short_name = 'BT'
                hub = Data_hub_call_restful_bt(request)
                hub_response = hub.call_api_fetch(request.params, get_latest_only=self.get_latest_only)
            elif request.api_core_url == Data_hub_call_osisoft_pi.core_URL:
                short_name = 'Triangulum'
                hub = Data_hub_call_osisoft_pi(request)
                hub_response = hub.call_api_fetch(get_latest_only=self.get_latest_only)
            elif request.api_core_url == Data_hub_call_restful_cdp.core_URL:
                short_name = 'CDP'
                hub = Data_hub_call_restful_cdp(request)
                if 'time_field' in request.feed_info:
                    get_children_as_time_series = True
                    time_field = request.feed_info['time_field']
                else:
                    get_children_as_time_series = False
                    time_field = ''
                hub_response = hub.call_api_fetch(request.params,
                                                  get_latest_only=self.get_latest_only,
                                                  get_children_as_time_series=get_children_as_time_series,
                                                  time_field=time_field)

            csv_output_prefix = short_name + '_'
            print(short_name + ' hub response: ' + str(hub_response))
        except Exception as err:
            raise err



        print(short_name + " call successful. " + str(str(hub_response['returned_matches'])) +
              " returned rows from " + request.users_feed_name)

        if (self.influx_db != None):
            try:
                if request.api_core_url == Data_hub_call_restful_bt.core_URL:
                    self.influx_db.import_restful_bt_response_json(
                        hub_response['content'],
                        request.users_feed_name,
                        request.feed_info)
                elif request.api_core_url == Data_hub_call_osisoft_pi.core_URL:
                    self.influx_db.import_pi_response_json(
                        hub_response['content'],
                        request.users_feed_name,
                        request.feed_info)
                elif request.api_core_url == Data_hub_call_restful_cdp.core_URL:
                    self.influx_db.import_restful_cdp_response_json(
                        hub_response['content'],
                        request.users_feed_name,
                        request.feed_info)
                print("Influx-db call successful: Import to influx table: " +
                      request.users_feed_name + " in " + self.influxdb_db_name)
            except Exception as err:
                print("Error populating " + short_name + ": " + str(err))
            """else:
                try:
                    print('select * from ' + request.users_feed_name + ": " +
                          str(self.influx_db.query_database(
                              'select * from ' + request.users_feed_name + ';')))
                except Exception as err:
                    print("Error reading new data from influx-db.")"""

        else:
            file_spec = os.path.join(self.output_dir,
                                     csv_output_prefix + request.users_feed_name + '.json')
            try:
                with open(file_spec, 'a+') as csv_file:
                    csv_file.write(hub_response['content'] + '\n')
                print("csv file write successful: To file: " + file_spec)
            except Exception as err:
                print('Unable to write to output file .' + str(err))

