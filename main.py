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
            home_dir = sys.argv[1]  # ./data_sources
            db_type = sys.argv[2]  # influx
            db_name = sys.argv[3]  # BT_Feeds_Test_3
            db_host = sys.argv[4]  # localhost
            db_port = sys.argv[5]  # 8086
            db_user = sys.argv[6]  # root
            db_pw = sys.argv[7]  # root
            polling_interval = float(sys.argv[8])  # 5.0
            try:
                get_latest_only = sys.argv[9]  # y or n
            except:
                get_latest_only = ''
    except:
        print("Error reading command line arguments. " + str(sys.argv))
        print("1. input/output directory path (string)")
        print("2. database type: [influx'] (Will be a list eventually but currently 1)")
        print("3. [database name]")
        print("4. [database host]")
        print("5. [database port]")
        print("6. [database user]")
        print("7. [database password]")
        print("8. polling interval seconds (float)")
        print("4. Optional: Get new/latest results only (bool) [y or n] - Default = y")
        sys.exit(0)

    try:
        if (str(home_dir).strip() == ''):
            raise AttributeError('Error reading first parameter: input/output directory (string). Exiting')

        if (str(db_type).strip() == ''):
            raise AttributeError("Error reading second parameter: database type]. Exiting.")

        if (str(db_name).strip() == ''):
            raise AttributeError("Error reading third parameter: [database name]. Exiting.")

        if (str(db_host).strip() == ''):
            db_host = 'localhost'

        if (str(db_port).strip() == ''):
            db_port = 8086

        if (str(db_user).strip() == ''):
            raise AttributeError("Error reading third parameter: [database user-id]. Exiting.")

        if (str(db_pw).strip() == ''):
            raise AttributeError("Error reading third parameter: [database password]. Exiting.")

        if (str(get_latest_only).strip() == 'n'):
            bool_get_latest_only = False
        else:
            bool_get_latest_only = True

        poller = Restful_api_poller(home_dir, db_type, db_name, db_host, db_port, db_user, db_pw, polling_interval,
                                    bool_get_latest_only)
        poller.start()

    except Exception as err:
        print(str(err))
        sys.exit(0)

if __name__ == '__main__':
  main()
