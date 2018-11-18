import os

from data_hub_call.selectedStreamsFromFileHubs import SelectedStreamsFromFileHubs
from scheduled_poller.scheduled_poller import Scheduled_poller
from data_hub_call.dataHubCallFactory import DataHubCallFactory

try:

    from aafp_database_connection.databaseConnectionFactory import DatabaseConnectionFactory
except:
    pass



class IotApiPoller(Scheduled_poller):

    INPUT_DIR = 'data_sources'
    CSV_OUTPUT_DIR = 'output'
    DEFAULT_POLLER_ID = 'default-id'

    def __init__(self, force_file, home_dir, db_type, polling_interval, check_files=True, get_latest_only=True,
                 db_name=None, db_host=None, db_port=None, db_user=None, db_pw=None):
        if not os.path.isdir(home_dir):
            raise IsADirectoryError("Home directory entered: " + home_dir + " does not exist.")

        super(IotApiPoller, self).__init__()

        self.get_latest_only = get_latest_only
        self.check_files_each_poll = check_files
        self.requests_dir = os.path.join(home_dir, self.INPUT_DIR)
        self.output_dir = os.path.join(home_dir, self.CSV_OUTPUT_DIR)
        self.db_name = db_name
        self.db_type = db_type
        if not force_file and db_type.strip().lower() != 'file':
            self.db = DatabaseConnectionFactory.create_database_connection(self.db_type,
                                                                           self.db_name,
                                                                           db_host,
                                                                           db_port,
                                                                           db_user,
                                                                           db_pw)
        else:
            self.db = None

        if self.check_files_each_poll == False:
            try:
                self.selected_streams = SelectedStreamsFromFileHubs(self.requests_dir)
            except Exception as err:
                raise err

            if len(self.selected_streams.get_api_streams()) == 0:
                raise IOError('Unable to read any streams data from input home directory. Exiting.')

    def do_work(self):
        if self.check_files_each_poll == True:
            try:
                self.selected_streams = SelectedStreamsFromFileHubs(self.requests_dir)
            except Exception as err:
                print("Error loading streams during this poll: " + str(err))

            if len(self.selected_streams.get_api_streams()) == 0:
                print('Unable to read any streams data from input home directory during this poll.')
                return


        print("")
        print("***** No. of streams to be processed: " + str(len(self.selected_streams.get_api_streams())) + " *****")

        for request in self.selected_streams.get_api_streams():
            # poll API
            try:
                self.poll_hub(request)
            except Exception as err:
                print("Not able to poll " + request.users_feed_name + ' at this time. Continuing poller. ' + str(err))

    def poll_hub(self, request):
        try:
            hub = DataHubCallFactory.create_data_hub_call(request)
            hub_response = hub.call_api_fetch(request.params, get_latest_only=self.get_latest_only)
            print(hub.hub_id + ' hub response: ' + str(hub_response))
        except Exception as err:
            raise err

        print(hub.hub_id + " call successful. " + str(str(hub_response['returned_matches'])) +
              " returned rows from " + request.users_feed_name)

        if self.db is not None:
            try:
                self.db.import_json(
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
                                     request.hub_id + '_' + request.users_feed_name + '.csv')
            try:
                with open(file_spec, 'a+') as csv_file:
                    csv_file.write(hub.json_result_to_csv(hub_response['content'] + '\n'))
                print("csv file write successful: To file: " + file_spec)
            except Exception as err:
                print('Unable to write to output file ' + file_spec + '. ' + str(err))
