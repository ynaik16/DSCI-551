import mysql.connector
import sys

mydb = mysql.connector.connect(
  host="localhost",
  user="dsci551",
  password="Dsci-551",
  database="dsci551"
)

nx = sys.argv[1]


mycursor.execute("ALTER TABLE ChatLogs ADD FULLTEXT index_name(Person)")

mycursor.execute("SELECT Time,Message FROM ChatLogs WHERE MATCH(Person) AGAINST('%s' IN NATURAL LANGUAGE MODe)"%nx)

myresult = mycursor.fetchall()

for x in myresult:
  print(x)
