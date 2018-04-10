import sys
from restful_api_poller import Restful_api_poller

try:
    import influxdb_connection
    force_file = False
except ImportError:
    force_file = True


def main():
    try:
        if (len(sys.argv) > 0):
            home_dir = sys.argv[1]              #./data_sources
            influxdb_db_name = sys.argv[2]          #BT_Feeds_Test_3
            polling_interval = float(sys.argv[3])   #5.0
            try:
                get_latest_only = sys.argv[4]          #y or n
            except:
                get_latest_only = ''
    except:
        print("Error reading command line arguments. " + str(sys.argv))
        print("1. input/output directory path (string)")
        str_force_instruction = ''
        if(force_file):
            str_force_instruction = \
                'Warning: You do not have the influxdb_connection library installed, so this parameter will be ' \
                    + 'automatically changed to "file".'
        print("2. [influx database name] or 'file'. " + str_force_instruction)
        print("3. polling interval seconds (float)")
        print("4. Optional: Get new/latest results only (bool) [y or n] - Default = y")
        sys.exit(0)

    if(str(home_dir).strip() == ''):
        print('Error reading first parameter: input/output directory (string)')

    bool_get_latest_only = None

    if(force_file):
        influxdb_db_name = 'file'
    elif(str(influxdb_db_name).strip() == ''):
        print("Error reading second parameter: [influx database name] or 'file'" )
    elif (str(get_latest_only).strip() == 'n'):
        bool_get_latest_only = False
    else: #elif(str(get_latest).strip() == '' or str(get_latest).strip() == 'y'):
        bool_get_latest_only = True

    poller = Restful_api_poller(home_dir, influxdb_db_name, polling_interval, bool_get_latest_only)
    poller.start()


if __name__ == '__main__':
  main()
