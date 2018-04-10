import os
import sys

sys.path.insert(0, '../data_hub_call')
sys.path.insert(0, '../influxdb_connection')

from selected_streams import Selected_streams
from poller import Poller

from data_hub_call_restful_bt import Data_hub_call_restful_bt
from data_hub_call_restful_cdp import Data_hub_call_restful_cdp
from data_hub_call_osisoft_pi import Data_hub_call_osisoft_pi


RESTFUL_BT_SOURCES_DIR = "restful_bt_sources"
BT_HUB_CREDENTIALS_FILE = 'bt_hub_credentials.csv'
RESTFUL_BT_REQUESTS_FILE = 'list_restful_bt_requests.csv'
CDP_SOURCES_DIR = "cdp_sources"
CDP_CREDENTIALS_FILE = 'cdp_credentials.csv'
CDP_REQUESTS_FILE = 'list_cdp_requests.csv'
TRIANGULUM_SOURCES_DIR = 'osisoft_pi_sources'
TRIANGULUM_REQUESTS_FILE = 'list_osisoft-pi_requests.csv'


try:
    import influxdb_connection
    force_file = False
except ImportError:
    force_file = True


class Restful_api_poller(Poller):

    INPUT_DIR = 'data_sources'
    CSV_OUTPUT_DIR = 'output'


    def __init__(self, home_dir, influxdb_db_name, polling_interval, get_latest_only=True):
        if not os.path.isdir(home_dir):
            print("Home directory entered: " + home_dir + " does not exist. Exiting.")
            sys.exit()

        super(Restful_api_poller, self).__init__(polling_interval)

        self.get_latest_only = get_latest_only
        self.requests_dir = os.path.join(home_dir, self.INPUT_DIR)
        self.output_dir = os.path.join(home_dir, self.CSV_OUTPUT_DIR)
        self.influxdb_db_name = influxdb_db_name

        self.influx_db = None

        try:
            self.selected_streams = Selected_streams(self.requests_dir)
            self.selected_streams.get_streams_from_file()

        except Exception as err:
            print("Not able to open files in " + home_dir + '. Exiting.')
            sys.exit()


        if (len(self.selected_streams.api_streams.requests) == 0):
            raise FileNotFoundError('Unable to read any streams data from input home directory. Exiting.')


    def do_work(self):
        print("")
        print("***** No. of streams to be processed: " + str(len(self.selected_streams.api_streams.requests)) + " *****")

        if (str(self.influxdb_db_name).strip().lower() != 'file'):
            self.influx_db = influxdb_connection.Influxdb_connection(self.influxdb_db_name)

        for request in self.selected_streams.api_streams.requests:
            # poll API
            try:
                self.poll_hub(request)
            except Exception as err:
                print("Not able to poll " + request.users_feed_name + ' at this time. Continuing poller. ' + str(err))


    def poll_hub(self, request):
        if request.api_core_url == Data_hub_call_restful_bt.core_URL:
            self.poll_bt_hub(request)
        elif request.api_core_url == Data_hub_call_restful_cdp.core_URL:
            self.poll_cdp_hub(request)
        elif request.api_core_url == Data_hub_call_osisoft_pi.core_URL:
            self.poll_pi_hub(request)


    def poll_bt_hub(self, request):
        try:
            hub = Data_hub_call_restful_bt(request)
            hub_response = hub.call_api_fetch(request.params, get_latest_only=self.get_latest_only)
            print('BT hub response: ' + str(hub_response))
        except Exception as err:
            raise err

        print("BT call successful. " + str(str(hub_response['returned_matches'])) +
              " returned rows from " + request.users_feed_name)

        if (self.influx_db != None):
            try:
                self.influx_db.import_restful_bt_response_json(
                    hub_response['content'],
                    request.users_feed_name,
                    request.feed_info)
                print("Influx-db call successful: Import to influx table: " +
                      request.users_feed_name + " in " + self.influxdb_db_name)
            except Exception as err:
                print("Error populating BT: " + str(err))

        else:
            file_spec = os.path.join(self.output_dir,
                                     'BT_' + request.users_feed_name + '.json')
            try:
                with open(file_spec, 'a+') as csv_file:
                    csv_file.write(hub_response['content'] + '\n')
                print("csv file write successful: To file: " + file_spec)
            except Exception as err:
                print('Unable to write to output file .' + str(err))


    def poll_cdp_hub(self, request):
        try:
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
            print('CDP hub response: ' + str(hub_response))
        except Exception as err:
            raise err


        print("CDP call successful. " + str(str(hub_response['returned_matches'])) +
              " returned rows from " + request.users_feed_name)

        if (self.influx_db != None):
            try:
                self.influx_db.import_restful_cdp_response_json(
                    hub_response['content'],
                    request.users_feed_name,
                    request.feed_info)
                print("Influx-db call successful: Import to influx table: " +
                      request.users_feed_name + " in " + self.influxdb_db_name)
            except Exception as err:
                print("Error populating CDP: " + str(err))

        else:
            file_spec = os.path.join(self.output_dir,
                                     'CDP_' + request.users_feed_name + '.json')
            try:
                with open(file_spec, 'a+') as csv_file:
                    csv_file.write(hub_response['content'] + '\n')
                print("csv file write successful: To file: " + file_spec)
            except Exception as err:
                print('Unable to write to output file .' + str(err))

    def poll_pi_hub(self, request):
        try:
            hub = Data_hub_call_osisoft_pi(request)
            hub_response = hub.call_api_fetch(get_latest_only=self.get_latest_only)
            print('Triangulum hub response: ' + str(hub_response))
        except Exception as err:
            raise err

        print("Triangulum call successful. " + str(str(hub_response['returned_matches'])) +
              " returned rows from " + request.users_feed_name)

        if (self.influx_db != None):
            try:
                self.influx_db.import_pi_response_json(
                    hub_response['content'],
                    request.users_feed_name,
                    request.feed_info)
                print("Influx-db call successful: Import to influx table: " +
                      request.users_feed_name + " in " + self.influxdb_db_name)
            except Exception as err:
                print("Error populating Triangulum: " + str(err))

        else:
            file_spec = os.path.join(self.output_dir,
                                     'triangulum_' + request.users_feed_name + '.json')
            try:
                with open(file_spec, 'a+') as csv_file:
                    csv_file.write(hub_response['content'] + '\n')
                print("csv file write successful: To file: " + file_spec)
            except Exception as err:
                print('Unable to write to output file .' + str(err))