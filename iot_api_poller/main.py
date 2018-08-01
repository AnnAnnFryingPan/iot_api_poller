import sys

from iot_api_poller.iotApiPoller import IotApiPoller

try:
    import database_connection.databaseConnectionInflux
    force_file = False
except ImportError as err:
    print("Error importing Influx. Will attempt to write to CSV file instead")
    force_file = True


def main():
    try:
        if (len(sys.argv) > 0):
            home_dir = sys.argv[1]  # ./data_sources
            db_type = sys.argv[2]  # influx
            try:
                check_files = sys.argv[3]  # y or n
            except:
                check_files = ''
            polling_interval = float(sys.argv[4])  # 5.0
            try:
                get_latest_only = sys.argv[5]  # y or n
            except:
                get_latest_only = ''

            if force_file == False and db_type.lower().strip() != 'file':
                db_name = sys.argv[6]  # BT_Feeds_Test_3
                db_host = sys.argv[7]  # localhost
                db_port = sys.argv[8]  # 8086
                db_user = sys.argv[9]  # root
                db_pw = sys.argv[10]  # root
            else:
                db_name = None
                db_host = None
                db_port = None
                db_user = None
                db_pw = None


    except:
        print("Error reading command line arguments. " + str(sys.argv))
        print("Expected inputs: ")
        print("1. input/output directory path (string)")
        print("2. database type: [influx|file] (Will be a list of options eventually but currently just 'influx' or file)")
        print("3. Check the files each poll (bool) [y or n] - Default = y")
        print("4. polling interval seconds (float)")
        print("5. Get new/latest results only (bool) [y or n] - Default = y")
        if force_file == False:
            print("")
            print("If database type is not 'file' then the following options are required:")
            print("     6. [database name]")
            print("     7. [database host]")
            print("     8. [database port]")
            print("     9. [database user]")
            print("     10. [database password]")

        sys.exit(0)

    try:
        if str(home_dir).strip() == '':
            raise AttributeError('Error reading first parameter: input/output directory (string). Exiting')

        if str(db_type).strip() == '':
            raise AttributeError("Error reading second parameter: database type]. Exiting.")

        if str(check_files).strip() == 'n':
            bool_check_files = False
        else:
            bool_check_files = True

        if str(get_latest_only).strip() == 'n':
            bool_get_latest_only = False
        else:
            bool_get_latest_only = True

        if force_file == False and db_type.lower().strip() != 'file':
            if str(db_name).strip() == '':
                raise AttributeError("Error reading parameter: [database name]. Exiting.")
            if str(db_host).strip() == '':
                db_host = 'localhost'
            if str(db_port).strip() == '':
                db_port = 8086
            if str(db_user).strip() == '':
                raise AttributeError("Error reading parameter: [database user-id]. Exiting.")
            if str(db_pw).strip() == '':
                raise AttributeError("Error reading parameter: [database password]. Exiting.")



        poller = IotApiPoller(
            force_file,
            home_dir, db_type, polling_interval, bool_check_files, bool_get_latest_only, db_name, db_host, db_port, db_user, db_pw)
        poller.start()

    except Exception as err:
        print(str(err))
        sys.exit(0)

if __name__ == '__main__':
  main()
