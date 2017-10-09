import time
from tendo import singleton
import sys
import os
import json

sys.path.insert(0, '../data_hub_call')
sys.path.insert(0, '../influxdb_connection')

import influxdb_connection
from request_info_fetch_list import Request_info_fetch_list
from data_hub_call_bt import Data_hub_call_bt
from data_hub_call_osisoft_pi import Data_hub_call_osisoft_pi

me = singleton.SingleInstance() # will sys.exit(-1) if other instance is running

restful_bt_sources_dir = "restful_bt_sources"
bt_hub_credentials_file = 'bt_hub_credentials.csv'
restful_bt_requests_file = 'list_restful_bt_requests.csv'
triangulum_sources_dir = 'osisoft_pi_sources'
triangulum_requests_file = 'list_osisoft-pi_requests.csv'
input_dir = 'data_sources'
csv_output_dir = 'output'
csv_output_file_bt = 'bt_latest_output.csv'
csv_output_file_tri = 'triangulum_latest_output.csv'



def main():
    try:
        if (len(sys.argv) > 0):
            home_dir = sys.argv[1]              #./data_sources
            influxdb_db_name = sys.argv[2]          #BT_Feeds_Test_3
            polling_interval = float(sys.argv[3])   #5.0
            try:
                get_latest = sys.argv[4]          #f or t
            except:
                get_latest = ''
    except:
        print("Error reading command line arguments. " + str(sys.argv))
        print("1. input/output directory path (string)")
        print("2. [influx database name] or 'csv'")
        print("3. polling interval seconds (float)")
        print("4. Optional: Get new results only (bool) [y or n] - Default = y")
        sys.exit(0)

    if(str(home_dir).strip() == ''):
        print('Error reading first parameter: input/output directory (string)')
    if(str(influxdb_db_name).strip() == ''):
        print("Error reading second parameter: [influx database name] or 'csv'" )

    if(str(get_latest).strip() == '' or str(get_latest).strip() == 'y'):
        bool_get_latest = True
    elif(str(get_latest).strip() == 'n'):
        bool_get_latest = False


    api_requests = Request_info_fetch_list()
    requests_dir = os.path.join(home_dir, input_dir)
    output_dir = os.path.join(home_dir, csv_output_dir)


    try:
        with open(os.path.join(requests_dir, restful_bt_sources_dir, bt_hub_credentials_file)) as f_creds:
            credentials_file = f_creds.readlines()
            list_params = credentials_file[0].split(",")
            restful_bt_api_key = list_params[0]
            restful_bt_username = list_params[1].rstrip('\n')
    except Exception as err:
        print('Unable to read BT credentials file ' + bt_hub_credentials_file + ' file in ' + requests_dir +
              '. BT streams in ' + restful_bt_requests_file + ' will be ignored. ' + str(err))
    else:
        try:
            with open(os.path.join(requests_dir, restful_bt_sources_dir, restful_bt_requests_file)) as f_requests:
                api_requests_file = f_requests.readlines()
            api_requests.append_restful_bt_request_list(restful_bt_username, restful_bt_api_key, api_requests_file)
        except Exception as err:
            print('Unable to read BT streams file ' + restful_bt_requests_file + ' file in ' + restful_bt_sources_dir
                  + '. ' + str(err))


    try:
        with open(os.path.join(requests_dir, triangulum_sources_dir, triangulum_requests_file)) as f_requests:
            api_requests_file = f_requests.readlines()
        api_requests.append_pi_request_list(api_requests_file)
    except Exception as err:
        print('Unable to read Triangulum streams file ' + triangulum_requests_file + ' file in '
              + triangulum_sources_dir  + '. ' + str(err))

    if(len(api_requests) == 0):
        print('Unable to read any streams data from ' + requests_dir + '. Exiting.')
        sys.exit(0)


    influxdb_connection = None

    running = True
    while running:
        start = time.clock()

        print("")
        print("***** No. of streams to be processed: " + str(len(api_requests)) + " *****")

        if(str(influxdb_db_name).strip().lower() != 'csv'):
            influx_db_connection = influxdb_connection.Influxdb_connection(influxdb_db_name)

        for request in api_requests.requests:

            # poll API
            if(request.api_core_url == Data_hub_call_bt.core_URL):
                try:
                    bt_hub = Data_hub_call_bt(request)
                    cur_params = request.params

                    bt_hub_response = bt_hub.call_api_fetch(cur_params, get_latest_only=bool_get_latest)

                    if (bt_hub_response['ok']):
                        print("BT-Hub call successful. " + str(
                            len(bt_hub_response['content'])) + " returned rows from " + request.users_feed_name)
                        print(bt_hub_response['content'])

                        if(influxdb_connection != None):
                            try:
                                influx_db_connection.import_hypercat_response_json(bt_hub_response['content'],
                                                                                   request.users_feed_name)
                                print("influxDb call successful: To table: " +
                                      request.users_feed_name + " in " + influxdb_db_name)
                                print(influx_db_connection.query_database(
                                    'select value from ' + request.users_feed_name + ';'))
                            except Exception as err:
                                print("Error populating influx-db: " + str(err))
                        else:
                            try:
                                with open(os.path.join(output_dir, csv_output_file_bt), 'w+') as csv_file:
                                    json.dump(bt_hub_response, csv_file)
                            except Exception as err:
                                print('Unable to write to ' + csv_output_file_bt + ' in ' +
                                      output_dir + '. ' + str(err))



                    else:
                        print("Error: call to hub: " + request.api_core_url + " failed with status code: " + \
                               bt_hub_response.reason)

                except Exception as err:
                    print("Error calling Hub: " + request.api_core_url + ". " + str(err))



            elif(request.api_core_url == Data_hub_call_osisoft_pi.core_URL):
                try:
                    pi_hub = Data_hub_call_osisoft_pi(request)
                    pi_hub_response = pi_hub.call_api_fetch()
                    print('Pi hub response: ' + str(pi_hub_response))
                    if (pi_hub_response['ok']):
                        json_content = json.loads(pi_hub_response['content'])
                        print("Pi-Hub call successful. " + str(len(json_content['Items'])) \
                        + " returned rows from " + request.users_feed_name)

                        if(influxdb_connection != None):
                            try:
                                influx_db_connection.import_pi_response_json(pi_hub_response['content'],
                                                                             request.users_feed_name)
                                print("influxDb call successful: To table: " +
                                      request.users_feed_name + " in " + influxdb_db_name)
                                print('select value from ' + request.users_feed_name + ": " +
                                      str(influx_db_connection.query_database(
                                          'select value from ' + request.users_feed_name + ';')))
                            except Exception as err:
                                print("Error populating influx-db: " + str(err))
                        else:
                            try:
                                with open(os.path.join(output_dir, csv_output_file_tri), 'w+') as csv_file:
                                    csv_file.write(json.dumps(pi_hub_response))
                            except Exception as err:
                                print('Unable to write to ' + csv_output_file_tri + ' in ' +
                                      output_dir  + '. ' + str(err))
                                raise

                    else:
                        print("Error: call to hub: " + request.api_core_url + " failed with status code:  " + \
                              pi_hub_response.reason)

                    #print(pi_hub_response)
                except Exception as err:
                    print("Error calling Pi hub: " + request.api_core_url + ". " + str(err))



        work_duration = time.clock() - start
        time.sleep(polling_interval - work_duration)


if __name__ == '__main__':
  main()
