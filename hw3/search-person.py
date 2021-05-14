import mysql.connector
import sys

mydb = mysql.connector.connect(
  host="localhost",
  user="dsci551",
  password="Dsci-551",
  database="dsci551"
)

nx = sys.argv[1]


mycursor.execute("ALTER TABLE Roster ADD FULLTEXT index_name(Name)")
mycursor.execute("SELECT Name FROM Roster WHERE MATCH(Name) AGAINST('%s' IN NATURAL LANGUAGE MODE)"%nx)

myresult = mycursor.fetchall()

for x in myresult:
  print(x)
