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
    temp_data[0] = temp_data[0].replace('"', '').replace('-', '_').replace('&', '')

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

## Retrive Data from the TABLES

# Get the Security Names from the list.txt file
file_list = open("list.txt", "r").readlines()
security_list = list(map(lambda x : x.strip().lower(), file_list))
security_list.sort()

# connect with the psql
try:
	conn = psql.connect(database = DB_NAME, user = DB_USER, password = DB_PASS, host = DB_HOST, port = DB_PORT)
	print("Database connected Successful for Retrival")
except:
	print("Database not connected Successful for Retrival")
     	
# Create Cursor
cursor = conn.cursor()

# Declare Three Matrices
price = []
percentage = []
color = []

# IMP - To get all colors change limit to 31

for symbol in security_list:
    cursor.execute("""
    WITH historical_prices_cte AS (
    SELECT
        date, close, LAG(close) OVER (ORDER BY date) AS prev_close
    FROM (
        SELECT
            date, close
        FROM {}
        ORDER BY date DESC
        LIMIT 30
        )
    ORDER BY date ASC
    ),

    percentage_cte AS (
        SELECT
            date,
            close,
            prev_close, (close - prev_close) AS change,
            ROUND((((close - prev_close) / prev_close) * 100), 2) AS per_change
        FROM historical_prices_cte
    ),

    cummulative_percent_cte AS (
        SELECT
            date,
            close,
            change,
            per_change,
            SUM(change) OVER (ORDER BY date ASC) AS cumm_change,
            SUM(per_change) OVER (ORDER BY date ASC) AS cumm_percent_change
        FROM percentage_cte
    )

    SELECT
        date,
        close,
        change,
        per_change,
        cumm_percent_change,
        CASE
            WHEN cumm_percent_change > 40 THEN '#004c00'
            WHEN cumm_percent_change >= 30 AND cumm_percent_change < 40 THEN '#006600'
            WHEN cumm_percent_change >= 25 AND cumm_percent_change < 30 THEN '#008000'
            WHEN cumm_percent_change >= 20 AND cumm_percent_change < 25 THEN '#009900'
            WHEN cumm_percent_change >= 15 AND cumm_percent_change < 20 THEN '#00b300'
            WHEN cumm_percent_change >= 10 AND cumm_percent_change < 15 THEN '#00cc00'
            WHEN cumm_percent_change >= 5 AND cumm_percent_change < 10 THEN '#00e600'
            WHEN cumm_percent_change >= 2 AND cumm_percent_change < 5 THEN '#00ff00'
            WHEN cumm_percent_change >= 1 AND cumm_percent_change < 2 THEN '#4dff4d'
            WHEN cumm_percent_change >= 0 AND cumm_percent_change < 1 THEN '#80ff80'
            WHEN cumm_percent_change >= -1 AND cumm_percent_change < 0 THEN '#ffb3b3'
            WHEN cumm_percent_change >= -2 AND cumm_percent_change < -1 THEN '#ff9999'
            WHEN cumm_percent_change >= -5 AND cumm_percent_change < -2 THEN '#ff8080'
            WHEN cumm_percent_change >= -10 AND cumm_percent_change < -5 THEN '#ff6666'
            WHEN cumm_percent_change >= -15 AND cumm_percent_change < -10 THEN '#ff4d4d'
            WHEN cumm_percent_change >= -20 AND cumm_percent_change < -15 THEN '#ff0000'
            WHEN cumm_percent_change >= -25 AND cumm_percent_change < -20 THEN '#e60000'
            WHEN cumm_percent_change >= -30 AND cumm_percent_change < -25 THEN '#cc0000'
            WHEN cumm_percent_change >= -40 AND cumm_percent_change < -30 THEN '#b30000'
            WHEN cumm_percent_change < -40 THEN '#990000'
        ELSE
            '#000000'
        END AS color
    FROM cummulative_percent_cte;
    """.format(symbol.replace('-', '_').replace('&', '')))

    query_data = cursor.fetchall()

    price_temp = []
    percentage_temp = []
    color_temp = []

    # Query Result - (Close Price, 1) (Cummulative Percentage Change, 4) (Color, 5)
    for i in range(0, len(query_data)):
        price_temp.append(query_data[i][1])
        percentage_temp.append(query_data[i][4])
        color_temp.append(query_data[i][5])

    price.append(price_temp)
    percentage.append(percentage_temp)
    color.append(color_temp)

## Writing HTML and CSS Files

# Create HTML File
html_file = open("test.html", 'w')

# Header Code
header = """

<!DOCTYPE html>
<html>
    <head>
        <title>NIFTY 50</title>
        <link rel = "stylesheet" href = "test.css">
    </head>

    <body>
        <div class = "header">
            <h1>Litmus</h1>
        </div>

        <div class = "color_div_1"><p class = "color_1">-40%</p></div>
        <div class = "color_div_2"><p class = "color_2">-35%</p></div>
        <div class = "color_div_3"><p class = "color_3">-30%</p></div>
        <div class = "color_div_4"><p class = "color_4">-25%</p></div>
        <div class = "color_div_5"><p class = "color_5">-20%</p></div>
        <div class = "color_div_6"><p class = "color_6">-15%</p></div>
        <div class = "color_div_7"><p class = "color_7">-10%</p></div>
        <div class = "color_div_8"><p class = "color_8">-5%</p></div>
        <div class = "color_div_9"><p class = "color_9">-2%</p></div>
        <div class = "color_div_10"><p class = "color_10">-1%</p></div>
        <div class = "color_div_11"><p class = "color_11">0%</p></div>
        <div class = "color_div_12"><p class = "color_12">1%</p></div>
        <div class = "color_div_13"><p class = "color_13">2%</p></div>
        <div class = "color_div_14"><p class = "color_14">5%</p></div>
        <div class = "color_div_15"><p class = "color_15">10%</p></div>
        <div class = "color_div_16"><p class = "color_16">15%</p></div>
        <div class = "color_div_17"><p class = "color_17">20%</p></div>
        <div class = "color_div_18"><p class = "color_18">25%</p></div>
        <div class = "color_div_19"><p class = "color_19">30%</p></div>
        <div class = "color_div_20"><p class = "color_20">40%</p></div>

        <div class = "date">
            <div class = "date_holder">
                <p>{}</p>
            </div>
        </div>

        <div class = "empty_spacer"></div>

        <div class = "stock_name">
            <p>SYMBOL</p>
        </div>

""".format(CURRENT_DATE)

# Index Code
index = """\n"""

for i in range(30):
    if ((29 - i) == 0):
        index += '        <div class = "index"><p>T</p></div>\n'
    else:
        index += '        <div class = "index"><p>T-{}</p></div>\n'.format(29 - i)

# Stock Prices Code
# IMP - To get all colors remove the IF condition

prices = """\n"""

for i in range(1, 101):
    prices += '\n        <div class = "stock_{}_name">\n'.format(i)
    prices += '            <p>{}</p>\n'.format(security_list[i - 1].upper())
    prices += '        </div>\n'

    for j in range(1, 31):
        if (j == 1):
            prices += """
        <div class = "stock_{}_price_{}">
            <p class = "price">{}</p>
        </div>
""".format(i, j, price[i - 1][j - 1])
        else:
            prices += """
        <div class = "stock_{}_price_{}">
            <p class = "price">{}</p>
            <p class = "percentage">{}%</p>
        </div>
""".format(i, j, price[i - 1][j - 1], percentage[i - 1][j - 1])

# Footer Code
footer = """\n"""

footer += """        <div class = "footer">
            <div class = "sub_footer">
                <p>Created by Shrikar Kaveti</p>
                <p>Dec 2024</p>
            </div>
        </div>
    </body>
</html>"""

# Write HTML File
html_file.write(header)
html_file.write(index)
html_file.write(prices)
html_file.write(footer)
html_file.close()

## Style Sheet

# Create CSS File
css_file = open('test.css', 'w')

style_header = """
* {
    margin: 0;
}

body {
    display: grid;
    background-color: black;
}

/* Title Bar Header */

.header {
    grid-column: 1 / 9;
    border-bottom: 1px whitesmoke solid;
    padding: 5px 20px;
}

/* Webpage Details */

.footer {
    grid-column: 1 / 32;
    margin: 30px 0px 0px 0px;
    border-top: 1px whitesmoke solid;
    font-size: large;
}

.sub_footer {
    padding: 10px 20px;
}

/* Header Colors */

/* -40% */
.color_div_1 {
    grid-column: 9 / 10;
    display: flex;
    justify-content: center;
    align-items: center;
    border-bottom: 1px solid whitesmoke;
}

.color_1 {
    padding: 5px 8px;
    border-radius: 5px;
    background-color: #990000;
}

/* -40% to -30% */
.color_div_2 {
    grid-column: 10 / 11;
    display: flex;
    justify-content: center;
    align-items: center;
    border-bottom: 1px solid whitesmoke;
}

.color_2 {
    padding: 5px 8px;
    border-radius: 5px;
    background-color: #b30000;
}

/* -30% to -25% */
.color_div_3 {
    grid-column: 11 / 12;
    display: flex;
    justify-content: center;
    align-items: center;
    border-bottom: 1px solid whitesmoke;
}

.color_3 {
    padding: 5px 8px;
    border-radius: 5px;
    background-color: #cc0000;
}

/* -25% to -20% */
.color_div_4 {
    grid-column: 12 / 13;
    display: flex;
    justify-content: center;
    align-items: center;
    border-bottom: 1px solid whitesmoke;
}

.color_4 {
    padding: 5px 8px;
    border-radius: 5px;
    background-color: #e60000;
}

/* -20% to -15% */
.color_div_5 {
    grid-column: 13 / 14;
    display: flex;
    justify-content: center;
    align-items: center;
    border-bottom: 1px solid whitesmoke;
}

.color_5 {
    padding: 5px 8px;
    border-radius: 5px;
    background-color: #ff0000;
}

/* 15% to -10% */
.color_div_6 {
    grid-column: 14 / 15;
    display: flex;
    justify-content: center;
    align-items: center;
    border-bottom: 1px solid whitesmoke;
}

.color_6 {
    padding: 5px 8px;
    border-radius: 5px;
    background-color: #ff4d4d;
}

/* -10% to -5% */
.color_div_7 {
    grid-column: 15 / 16;
    display: flex;
    justify-content: center;
    align-items: center;
    border-bottom: 1px solid whitesmoke;
}

.color_7 {
    padding: 5px 8px;
    border-radius: 5px;
    background-color: #ff6666;
}

/* -5% to -2% */
.color_div_8 {
    grid-column: 16 / 17;
    display: flex;
    justify-content: center;
    align-items: center;
    border-bottom: 1px solid whitesmoke;
}

.color_8 {
    padding: 5px 8px;
    border-radius: 5px;
    background-color: #ff8080;
}

/* -2% to -1% */
.color_div_9 {
    grid-column: 17 / 18;
    display: flex;
    justify-content: center;
    align-items: center;
    border-bottom: 1px solid whitesmoke;
}

.color_9 {
    padding: 5px 8px;
    border-radius: 5px;
    background-color: #ff9999;
}

/* -1% to 0% */
.color_div_10 {
    grid-column: 18 / 19;
    display: flex;
    justify-content: center;
    align-items: center;
    border-bottom: 1px solid whitesmoke;
}

.color_10 {
    padding: 5px 8px;
    border-radius: 5px;
    background-color: #ffb3b3;
}

/* 0% to 1% */
.color_div_11 {
    grid-column: 19 / 20;
    display: flex;
    justify-content: center;
    align-items: center;
    border-bottom: 1px solid whitesmoke;
}

.color_11 {
    padding: 5px 8px;
    border-radius: 5px;
    background-color: #80ff80;
}

/* 1% to 2% */
.color_div_12 {
    grid-column: 20 / 21;
    display: flex;
    justify-content: center;
    align-items: center;
    border-bottom: 1px solid whitesmoke;
}

.color_12 {
    padding: 5px 8px;
    border-radius: 5px;
    background-color: #4dff4d;
}

/* 2% to 5% */
.color_div_13 {
    grid-column: 21 / 22;
    display: flex;
    justify-content: center;
    align-items: center;
    border-bottom: 1px solid whitesmoke;
}

.color_13 {
    padding: 5px 8px;
    border-radius: 5px;
    background-color: #00ff00;
}

/* 5% to 10% */
.color_div_14 {
    grid-column: 22 / 23;
    display: flex;
    justify-content: center;
    align-items: center;
    border-bottom: 1px solid whitesmoke;
}

.color_14 {
    padding: 5px 8px;
    border-radius: 5px;
    background-color: #00e600;
}

/* 10% to 15% */
.color_div_15 {
    grid-column: 23 / 24;
    display: flex;
    justify-content: center;
    align-items: center;
    border-bottom: 1px solid whitesmoke;
}

.color_15 {
    padding: 5px 8px;
    border-radius: 5px;
    background-color: #00cc00;
}

/* 15% to 20% */
.color_div_16 {
    grid-column: 24 / 25;
    display: flex;
    justify-content: center;
    align-items: center;
    border-bottom: 1px solid whitesmoke;
}

.color_16 {
    padding: 5px 8px;
    border-radius: 5px;
    background-color: #00b300;
}

/* 20% to 25% */
.color_div_17 {
    grid-column: 25 / 26;
    display: flex;
    justify-content: center;
    align-items: center;
    border-bottom: 1px solid whitesmoke;
}

.color_17 {
    padding: 5px 8px;
    border-radius: 5px;
    background-color: #009900;
}

/* 25% to 30% */
.color_div_18 {
    grid-column: 26 / 27;
    display: flex;
    justify-content: center;
    align-items: center;
    border-bottom: 1px solid whitesmoke;
}

.color_18 {
    padding: 5px 8px;
    border-radius: 5px;
    background-color: #008000;
}

/* 30% to 40% */
.color_div_19 {
    grid-column: 27 / 28;
    display: flex;
    justify-content: center;
    align-items: center;
    border-bottom: 1px solid whitesmoke;
}

.color_19 {
    padding: 5px 8px;
    border-radius: 5px;
    background-color: #006600;
}

/*+40% */
.color_div_20 {
    grid-column: 28 / 29;
    display: flex;
    justify-content: center;
    align-items: center;
    border-bottom: 1px solid whitesmoke;
}

.color_20 {
    padding: 5px 8px;
    border-radius: 5px;
    background-color: #004c00;
}

/* Header Date */

.date {
    grid-column: 29 / 31;
    border-bottom: 1px whitesmoke solid;
    padding: 15px 0px;
    text-align: center;
}

.date_holder {
    border-radius: 1px;
    border: 1px whitesmoke solid;
}

/* Empty Spacer After Date in Header */

.empty_spacer {
    grid-column: 31 / 32;
    border-bottom: 1px whitesmoke solid;
}

/* Index Border Setting */

.stock_name {
    border-bottom: 1px whitesmoke solid;
    border-left: 1px whitesmoke solid;
    border-right: 1px whitesmoke solid;
    text-align: center;
}

.index {
    border-bottom: 1px whitesmoke solid;
    border-right: 1px whitesmoke solid;
    text-align: center;
}

/* Used in the Table and Date */

p {
    color: whitesmoke;
    font-family: monospace;
}

/* Used in Header */

h1 {
    color: whitesmoke;
    font-family: monospace;
    font-style: italic;
}

/* Percentage in Table */

.percentage {
    font-style: italic;
    font-size: smaller;
}

/* Stock Name, Color and Price Data*/

"""

# Style Price
style_price = """"""

for i in range(1, 101):
    style_price += '.stock_{}_name '.format(i) + '{'
    style_price += """
    border-left: 1px whitesmoke solid;
    border-right: 1px whitesmoke solid;
    border-bottom: 1px whitesmoke solid;
    text-align: center;
    align-content: center;
}\n
"""
    
    for j in range(1, 31):
        style_price += '.stock_{}_price_{} '.format(i, j) + '{'
        style_price += """
    border-right: 1px whitesmoke solid;
    border-bottom: 1px whitesmoke solid;
    text-align: center;
    align-content: center;

    background-color: {};""".format(color[i - 1][j - 1]) + '\n}\n'

css_file.write(style_header)
css_file.write(style_price)

css_file.close()
