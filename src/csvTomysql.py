import os 
import sys
import glob
import pandas as pd

import csv
import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd=""
)

mycursor = mydb.cursor()

mycursor.execute("CREATE DATABASE IF NOT EXISTS csv_db")
script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.') 
csv_path = os.path.join(script_dir, 'DATA_CSV')
os.chdir(csv_path)
extension = "csv"
all_filenames = [i for i in glob.glob('*{}'.format(extension))]
mydb = mysql.connector.connect(host = "localhost", user = 'root', passwd = '', database='csv_db', port="3306")
cursor = mydb.cursor()
print(all_filenames)
for filename in all_filenames:
    datas = []
    with open(filename) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        titles = []
        for row in readCSV:
            data = []
            title = row[0].lower()
            titles.append(title)
            for index in range(1, len(row)):
                data.append(row[index])
            datas.append(data)

        creatQueryString = ""
        valFormat = ""
        fieldName = ""
        insertQueryString = 'INSERT INTO `' + filename.replace('.csv', '').lower() + '`('
        i = 1
        while 1:
            tit = titles[20*(i-1):20*i]
            if not tit:
                break
            for string in tit:
                creatQueryString += "`" + str(string) + "`" + " TEXT,"
                valFormat += "%s,"
                fieldName += "`" + str(string) + "`" +", "
            valFormat = valFormat[:-1]
            fieldName = fieldName[:-2]
            creatQueryString = creatQueryString[:-1]
            insertQueryString = insertQueryString + fieldName +") " + "VALUES (" + valFormat + ")"
            if len(titles) < 20:
                index = ""
            else:
                index = "("+str(i)+")"
            creatQueryString = "CREATE TABLE IF NOT EXISTS `" + filename.replace('.csv','').lower() + index + "`(" + creatQueryString +")"
            
            cursor.execute(creatQueryString)
            x = []
            for j in range(len(datas[0])):
                x.append([datas[i][j] for i in range(20*(i-1),20*i)])
            result = []
            for i in x:
                result.append(tuple(i))
            for i in range(0,len(result)):
                if result[100*i:100*(i+1)]:
                    cursor.executemany(insertQueryString, result[100*i:100*(i+1)])
            mydb.commit()
            i += 1
    print("importing successfully finished")