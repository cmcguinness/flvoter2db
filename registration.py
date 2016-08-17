import os
import pymysql.cursors
import csv


class Registration:
    """
        Load voter registration data into a table
    """
    def __init__(self, connection, dbname):
        """
        Initialize the class with
        :param connection:      a pymsql database connection
        :param dbname:          the name of the database (aka schema) to create our table in
        """
        self.connection = connection
        self.dbname = dbname


    def create_table(self):
        """
        Create a brand new registration table in the database.  Drop the old one without
        warning if it exists (that's probably a bit aggressive, but who knows what its
        schema looks like?)
        """

        sql = """
            CREATE TABLE `{}`.`reg` (
              `County` char(3) NOT NULL,
              `VoterID` int(10) unsigned NOT NULL,
              `Last` varchar(30) DEFAULT NULL,
              `Suffix` varchar(5) DEFAULT NULL,
              `First` varchar(30) DEFAULT NULL,
              `Middle` varchar(30) DEFAULT NULL,
              `Exempt` char(1) DEFAULT NULL,
              `ResAdr1` varchar(64) DEFAULT NULL,
              `ResAdr2` varchar(64) DEFAULT NULL,
              `ResCity` varchar(64) DEFAULT NULL,
              `ResState` char(2) DEFAULT NULL,
              `ResZip` char(12) DEFAULT NULL,
              `MailAdr1` varchar(64) DEFAULT NULL,
              `MailAdr2` varchar(64) DEFAULT NULL,
              `MailAdr3` varchar(64) DEFAULT NULL,
              `MailCity` varchar(64) DEFAULT NULL,
              `MailState` char(2) DEFAULT NULL,
              `MailZip` char(12) DEFAULT NULL,
              `MailCountry` varchar(64) DEFAULT NULL,
              `Gender` char(1) DEFAULT NULL,
              `Race` char(1) DEFAULT NULL,
              `BirthDate` date DEFAULT NULL,
              `RegDate` date DEFAULT NULL,
              `Party` char(3) DEFAULT NULL,
              `Precinct` char(6) DEFAULT NULL,
              `PrecinctGroup` char(3) DEFAULT NULL,
              `PrecinctSplit` char(6) DEFAULT NULL,
              `PrecinctSuffix` char(3) DEFAULT NULL,
              `VoterStatus` char(3) DEFAULT NULL,
              `Congress` char(3) DEFAULT NULL,
              `House` char(3) DEFAULT NULL,
              `Senate` char(3) DEFAULT NULL,
              `CountyCommission` char(3) DEFAULT NULL,
              `School` char(2) DEFAULT NULL,
              `Areacode` char(3) DEFAULT NULL,
              `Phone` char(7) DEFAULT NULL,
              `Extension` char(4) DEFAULT NULL,
              `Email` varchar(100) DEFAULT NULL,
              PRIMARY KEY (`VoterID`),
              KEY `CountyId` (`County`,`VoterID`),
              KEY `RegDate` (`RegDate`),
              KEY `BirthDate` (`BirthDate`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8
        """.format(self.dbname)
        with self.connection.cursor() as cursor:
            try:
                cursor.execute("DROP TABLE `{}`.`reg`".format(self.dbname))
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
            INSERT INTO `{}`.`reg`
                (`County`,
                `VoterID`,
                `Last`,
                `Suffix`,
                `First`,
                `Middle`,
                `Exempt`,
                `ResAdr1`,
                `ResAdr2`,
                `ResCity`,
                `ResState`,
                `ResZip`,
                `MailAdr1`,
                `MailAdr2`,
                `MailAdr3`,
                `MailCity`,
                `MailState`,
                `MailZip`,
                `MailCountry`,
                `Gender`,
                `Race`,
                `BirthDate`,
                `RegDate`,
                `Party`,
                `Precinct`,
                `PrecinctGroup`,
                `PrecinctSplit`,
                `PrecinctSuffix`,
                `VoterStatus`,
                `Congress`,
                `House`,
                `Senate`,
                `CountyCommission`,
                `School`,
                `Areacode`,
                `Phone`,
                `Extension`,
                `Email`)
            VALUES
              (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            if len(r[17]) > 12:
                print("Mail Zip code too long: {}".format(r[17]))
                r[17] = r[17][0:12]

            r[21] = self.date_reformat(r[21])
            r[22] = self.date_reformat(r[22])

            self.insert_row(r)
