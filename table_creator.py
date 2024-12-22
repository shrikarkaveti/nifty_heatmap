# import required libraries
import re
import glob
import psycopg2 as psql

# Get the Security Names from the list.txt file
file_list = open("list.txt", "r").readlines()
security_list = list(map(lambda x : x.strip().lower(), file_list))
security_list.sort()

if ('' in security_list):
    raise Exception("List has NULL VALUES")

# print(security_list)

# CSV Files
csv_files = glob.glob("security_price_archive/*.csv")

# connect with the psql
DB_NAME = "db_name"
DB_USER = "postgres"
DB_PASS = "password"
DB_HOST = "localhost"
DB_PORT = "port_number"

try:
	conn = psql.connect(database = DB_NAME, user = DB_USER, password = DB_PASS, host = DB_HOST, port = DB_PORT)
	print("Database connected Successful")
except:
	print("Database not connected Successful")
	
missing_files = []
security_csv_file_dict = {}
	
# Checking for the Missing Data
for i in security_list:
	check = 0
	
	for j in csv_files:
		if (re.search(i, j, re.IGNORECASE)):
			security_csv_file_dict.update({i : j})
			check = 1
			
	if (check == 0):
		missing_files.append(i)
		
# Alert When Files are Missing
if (len(missing_files) != 0):
	print("Missing Files")
	print("-------------")
	for i in missing_files:
	    print(i)
else:
	print("Required Files are Found...\nCreating DB")
		

# # Create Cursor
cursor = conn.cursor()

# # Create Table for each Security
for i in security_list:
	# Checking if the Table alredy exists
    cursor.execute("SELECT to_regclass('{}')".format(i))
    result = cursor.fetchall()
    table_exists = 0
	
    if (result[0][0] != None):
        table_exists = 1

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS {} (
        id SERIAL PRIMARY KEY,
        open NUMERIC NOT NULL,
        high NUMERIC NOT NULL,
        low NUMERIC NOT NULL,
        close NUMERIC NOT NULL,
        volume INT NOT NULL,
        value NUMERIC NOT NULL,
        date DATE NOT NULL,
        UNIQUE(date)
    );
    """.format(i))

    # Read the CSV File and collect Data
    data = open(security_csv_file_dict[i], "r").readlines()
    del data[0]
	
    if (table_exists == 0):
        # Index [(2, Date) (4, open) (5, high) (6, low) (7, close) (10, volume) (11, value)]

        # Create the Insert Command
        insert_data = """INSERT INTO {} (open, high, low, close, volume, value, date) VALUES """.format(i)

        for row in data:
            row_data = row.split('","')
            insert_data += "(REPLACE('{}', ',', '')::numeric, REPLACE('{}', ',', '')::numeric, REPLACE('{}', ',', '')::numeric, REPLACE('{}', ',', '')::numeric, REPLACE('{}', ',', '')::numeric, REPLACE('{}', ',', '')::numeric, DATE('{}')),".format(row_data[4], row_data[5], row_data[6], row_data[7], row_data[10], row_data[11], row_data[2])
            # print("({}, {}, {}, {}, REPLACE('{}', ',', '')::numeric, REPLACE('{}', ',', '')::numeric, DATE('{}'))".format(row_data[4], row_data[5], row_data[6], row_data[7], row_data[10], row_data[11], row_data[2]))

        insert_data = insert_data[:-1] + ';'
        
        # print(insert_data)
        
        # Inserting CSV data into the TABLE
        cursor.execute(insert_data)
        
        print("Created and Inserted Data into {} Successfully".format(i))
    
    else:
        print("EXISTS: {} already Exists".format(i))
	
    conn.commit()

conn.close()
