import os
import sys

sys.path.insert(0, '../data_hub_call')
sys.path.insert(0, '../poller')
sys.path.insert(0, '../database_connection')

from selected_streams import Selected_streams
from poller import Poller
from data_hub_call_factory import Data_hub_call_factory
from databaseConnectionFactory import DatabaseConnectionFactory


class Restful_api_poller(Poller):

    INPUT_DIR = 'data_sources'
    CSV_OUTPUT_DIR = 'output'
    DEFAULT_POLLER_ID = 'default-id'

    def __init__(self, home_dir, db_type, db_name, db_host, db_port, db_user, db_pw, polling_interval, get_latest_only=True):
        if not os.path.isdir(home_dir):
            raise IsADirectoryError("Home directory entered: " + home_dir + " does not exist.")

        super(Restful_api_poller, self).__init__(polling_interval)

        self.get_latest_only = get_latest_only
        self.requests_dir = os.path.join(home_dir, self.INPUT_DIR)
        self.output_dir = os.path.join(home_dir, self.CSV_OUTPUT_DIR)
        self.db_name = db_name
        self.db_type = db_type
        if str(self.db_name).strip().lower() != 'file':
            self.db = DatabaseConnectionFactory.create_database_connection(self.db_type,
                                                                           self.db_name,
                                                                           db_host,
                                                                           db_port,
                                                                           db_user,
                                                                           db_pw)

        try:
            self.selected_streams = Selected_streams(self.requests_dir)
            self.selected_streams.get_streams_from_file()
        except Exception as err:
            raise err

        if len(self.selected_streams.api_streams.requests) == 0:
            raise IOError('Unable to read any streams data from input home directory. Exiting.')

    def do_work(self):
        print("")
        print("***** No. of streams to be processed: " + str(len(self.selected_streams.api_streams.requests)) + " *****")

        for request in self.selected_streams.api_streams.requests:
            # poll API
            try:
                self.poll_hub(request)
            except Exception as err:
                print("Not able to poll " + request.users_feed_name + ' at this time. Continuing poller. ' + str(err))

    def poll_hub(self, request):
        try:
            hub = Data_hub_call_factory.create_data_hub_call(request)
            hub_response = hub.call_api_fetch(request.params, get_latest_only=self.get_latest_only)
            print(hub.hub_id + ' hub response: ' + str(hub_response))
        except Exception as err:
            raise err

        print(hub.hub_id + " call successful. " + str(str(hub_response['returned_matches'])) +
              " returned rows from " + request.users_feed_name)

        if self.db is not None:
            try:
                self.db.import_restful_api_response(
                    hub.get_influx_db_import_json(
                        hub_response['content'],
                        request.users_feed_name,
                        request.feed_info))
                print("DB call successful: Import to table: " +
                      request.users_feed_name + " in " + self.db_name)
            except Exception as err:
                print("Error populating DB with " + request.hub_id + " data: " + str(err))

        else:
            file_spec = os.path.join(self.output_dir,
                                     request.hub_id + '_' + request.users_feed_name + '.json')
            try:
                with open(file_spec, 'a+') as csv_file:
                    csv_file.write(hub_response['content'] + '\n')
                print("csv file write successful: To file: " + file_spec)
            except Exception as err:
                print('Unable to write to output file ' + file_spec + '. ' + str(err))