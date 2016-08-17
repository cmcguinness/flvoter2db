import pymysql.cursors
import csv


class History():
    """
        Load voting history into a table.
    """

    def __init__(self, connection, dbname, minyear):
        """
        Initialize the class with:
        :param connection:      a pymysql database connection
        :param dbname:          the name of the database to put the history table into
        :param minyear:         The lowest year's data we'll save to the table (to save space)
        """
        self.connection = connection
        self.minyear = minyear
        self.dbname = dbname


    def create_table(self):
        """
        Create a brand new history table in the database.  Drop the old one without
        warning if it exists (that's probably a bit aggressive, but who knows what its
        schema looks like?)
        """
        sql = """
            CREATE TABLE `{}`.`history` (
                  `County` char(3) NOT NULL,
                  `VoterID` int(10) NOT NULL,
                  `ElectionDate` date DEFAULT NULL,
                  `ElectionType` char(3) DEFAULT NULL,
                  `VotingType` char(1) DEFAULT NULL,
                  KEY `Date` (`ElectionDate`),
                  KEY `CountyID` (`County`,`VoterID`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
          """.format(self.dbname)

        with self.connection.cursor() as cursor:
            try:
                cursor.execute("DROP TABLE `{}`.`history`".format(self.dbname))
            except:
                pass  # Don't care ..

            cursor.execute(sql)

        self.connection.commit()


    def insert_row(self, row):
        """
        Insert a row of data into the database

        :param row:     The data, which is assumed to match perfectly the order of columns in the table
        """
        sql = """
            INSERT INTO `{}`.`history`
                (`County`,
                `VoterID`,
                `ElectionDate`,
                `ElectionType`,
                `VotingType`)
            VALUES
              (%s, %s, %s, %s, %s)
        """.format(self.dbname)

        with self.connection.cursor() as cursor:
            try:
                cursor.execute(sql, row)
            except:
                print("Exception inserting row ", row)

    @staticmethod
    def date_reformat(mmddyyyy):
        """
        Reformat a date from the format used in the CSV file to a MySQL format
        :param mmddyyyy:    Date in mm/dd/yyyy string format
        :return:            Date in yyyy-mm-dd string format
        """
        if len(mmddyyyy) == 0:
            return None
        return mmddyyyy[6:] + '-' + mmddyyyy[0:2] + '-' + mmddyyyy[3:5]

    def read_file(self, fname):
        """
        Read the given history csv file and load its data into the database
        :param fname:   File name holding CSV data
        """
        file = open(fname, "r")
        reader = csv.reader(file, delimiter='\t')
        for r in reader:
            if int(r[2][6:]) >= self.minyear:
                r[2] = self.date_reformat(r[2])
                self.insert_row(r)
