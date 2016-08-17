import os
import pymysql
import registration
import history
import configparser


"""

    Simple Python Program to read Florida voter data and load it into a MySQL database

    Author:     Charles McGuinness, cmcguinness on GitHub, github@mcguinness.us
    License:    See License.md file, but, basically, have at it!

    Description:

    The state of Florida release regular, monthly updates of both the voter rolls
    and voter history. The amount of data is fairly large (not "big", but still
    large) and separated into multiple county-specific files.

    This program will process each of those files and load them into two tables to
    allow you to do further analysis.
"""

class LoadVoterData():
    def __init__(self, config):
        self.dbinfo = config['database']

        self.connection = pymysql.connect(host=self.dbinfo['host'],
                                          user=self.dbinfo['user'],
                                          password=self.dbinfo['pass'],
                                          db=self.dbinfo['db'],
                                          cursorclass=pymysql.cursors.DictCursor)
        return      # Unneeded, but nice to demark the end

    def load_registration(self,dir):
        """
            Load the voter registration data.  All the work is handled by the Registration Class

            Argument is the directory that holds all of the county-by-county voter registration files.
        """
        reg = registration.Registration(self.connection, self.dbinfo['db'])

        reg.create_table()

        from_files = [f for f in os.listdir(dir) if f.endswith(".txt")]

        for file in from_files:
            print("Processing {}".format(file))
            reg.read_file(os.path.join(dir, file))
            self.connection.commit()

        return  # Unneeded, but nice to demark the end

    def load_history(self,dir):
        """
            Load the voter history data.  Note that we pass in a cutoff year for the history data
            (only elections from that year on) to limit the number of rows we generate...

            Argument is the directory that holds all of the county-by-county voter history files.
        """
        hist = history.History(self.connection, self.dbinfo['db'], 2016)

        hist.create_table()

        from_files = [f for f in os.listdir(dir) if f.endswith(".txt")]

        for file in from_files:
            print("Processing {}".format(file))
            hist.read_file(os.path.join(dir, file))
            self.connection.commit()


        return  # Unneeded, but nice to demark the end

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('cv2db.ini')
    main = LoadVoterData(config)
    # main.load_registration('/Users/charles/Projects/VoterFile/201608')
    main.load_history('/Users/charles/Projects/VoterFile/201608 History')