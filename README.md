# Load raw Florida voter data into a database
Charles McGuinness  
github@mcguinness.us  
@socialseercom

This is a simple program that reads the voter data provided by the state of Florida
and loads it into a MySQL database.  The data is given in a series of tab-delimited
text files, one per county, and the program will process all such .txt files in
a specified directory.

To run the program, first copy cv2db-example.ini to cv2db.ini, and then edit it to
have the appropriate values for your database server.  Then modify main.py to
point to the correct directories for the files.

Other than that, the program is fairly simple.