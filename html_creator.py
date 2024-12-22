# Import Required Libraries
import glob
import psycopg2 as psql

# Reading the CSV File and converting into strings
nifty_file = open(glob.glob("*.csv")[0], 'r').readlines()

# Deleting Unneccesary 15 lines
for i in range(15):
    del nifty_file[0]

# Deleting "NIFTY 100" data in nifty_file
del nifty_file[1]

# Reading Current Date from the CSV File
# CURRENT_DATE = nifty_file[0].strip().replace('"', '') - This calculates Date using the 365 Days difference which may not work always

CURRENT_DATE = glob.glob("*.csv")[0][13:24]
# print(CURRENT_DATE)

# Delete the Date in nifty_file
del nifty_file[0]

# connect with the psql
DB_NAME = "DB_name"
DB_USER = "postgres"
DB_PASS = "password"
DB_HOST = "localhost"
DB_PORT = "port_number"

try:
	conn = psql.connect(database = DB_NAME, user = DB_USER, password = DB_PASS, host = DB_HOST, port = DB_PORT)
	print("Database connected Successful")
except:
	print("Database not connected Successful")
     
# print(nifty_file[0].strip().replace(',', '').split('""'))
	
# Create Cursor
cursor = conn.cursor()

# Iterating through the CSV File Data
for i in range(len(nifty_file)):
    temp_data = nifty_file[i].strip().replace(',', '').split('""')
    temp_data[0] = temp_data[0].replace('"', '').lower()

    # print(nifty_file[i].strip().replace(',', '').split('""'))

    # [(symbol, 0) (open, 1) (high, 2) (low, 3) (close, 5) (volume, 9) (value, 10)]

    # Check Whether the Table Exists
    cursor.execute("SELECT to_regclass('{}')".format(temp_data[0]))
    result = cursor.fetchall()
    table_exists = 0

    if (result[0][0] != None):
        table_exists = 1

    if (table_exists == 1):
        cursor.execute("SELECT EXISTS (SELECT 1 FROM {} WHERE date = DATE('{}'));".format(temp_data[0], CURRENT_DATE))
        rows = cursor.fetchall()

        if (rows[0][0] == False):
            # Create the Insert Command
            insert_data = """INSERT INTO {} (open, high, low, close, volume, value, date) VALUES """.format(temp_data[0])
            insert_data += "(REPLACE('{}', ',', '')::numeric, REPLACE('{}', ',', '')::numeric, REPLACE('{}', ',', '')::numeric, REPLACE('{}', ',', '')::numeric, REPLACE('{}', ',', '')::numeric, (REPLACE('{}', ',', '')::numeric * pow(10, 7)), DATE('{}'));".format(temp_data[1], temp_data[2], temp_data[3], temp_data[5], temp_data[9], temp_data[10], CURRENT_DATE)

            cursor.execute(insert_data)

            print("Data Added - {} - Successful".format(temp_data[0]))

            conn.commit()

        else:
            print("Data Exists - {} - Successful".format(temp_data[0]))
    else:
        print("Missing Table - {}".format(temp_data[0]))
	
conn.close()

## 
