import json
import pymysql
from pymysql import cursors

# Storing in MySql Database using SQL
# Connecting to the Database
my_host = 'localhost'
my_user = 'root'
my_password = 'actowiz'
my_database = 'booktoscrape'
my_charset = 'utf8mb4'

connection = pymysql.connect(host=my_host, user=my_user, database=my_database, password=my_password, charset=my_charset, cursorclass=pymysql.cursors.DictCursor)
cursor = connection.cursor()

print('Reading from Database')
read_query = f'''SELECT * FROM `bookscrape`;'''
cursor.execute(query=read_query)

output = cursor.fetchall()

# Creating a JSON file after reading data from Database
with open('Jsondata.json', 'w') as file:
    final_output = []
    category = {}
    for data in output:
        data['Product_Price'] = float(data.get('Product_Price'))
        data['avg_rating'] = float(data.get('avg_rating'))
        if data.get('product_category') not in category:
            category[data.get('product_category')] = [data]
        else:
            category[data.get('product_category')].append(data)
    final_output.append(category)
    file.write(json.dumps(final_output))
