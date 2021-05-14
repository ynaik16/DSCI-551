import mysql.connector
import pandas as pd
import sys
from mysql.connector import errorcode

file1 = sys.argv[1]
file2 = sys.argv[2]

data1 = pd.read_csv(file1, error_bad_lines=False, header= None, delimiter = '\t')

data1.columns =['Time', 'Person', 'Message']


data2 = pd.read_csv(file2, error_bad_lines=False, delimiter = ',')
data2.columns =['Name', 'Country']
data2[['Last Name','First Name']] = data2['Name'].str.split(',',expand=True)
data2 = data2.drop(columns='Name', axis =1)
data2['Name'] = data2["First Name"] + ' ' + data2['Last Name']
data2= data2.drop(columns = ["Last Name", "First Name"], axis = 1)


mydb = mysql.connector.connect(
  host="localhost",
  user="dsci551",
  password="Dsci-551",
  database="dsci551"
)

mycursor = mydb.cursor()

mycursor.execute("CREATE TABLE Roster (Time VARCHAR(255), Person VARCHAR(255), Message VARCHAR(255))")


sql = "INSERT INTO ChatLogs (Time, Person, Message) VALUES (%s,%s, %s)"
for i in range(len(data1)):
    #input_vales(data1['Time'][i], data1['Person'][i], data1['Message'][i])
    val = (data1['Time'][i], data1['Person'][i], data1['Message'][i])
    mycursor.execute(sql, val)

mydb.commit()



mycursor.execute("CREATE TABLE Roster (Name VARCHAR(255), Country VARCHAR(255))")

sql = "INSERT INTO Roster (Name, Country) VALUES (%s, %s)"
for i in range(len(data2)):
    #input_vales(data1['Time'][i], data1['Person'][i], data1['Message'][i])
    val = (data2['Name'][i], data2['Country'][i])
    mycursor.execute(sql, val)

mydb.commit()









