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

mycursor.execute("CREATE DATABASE IF NOT EXISTS OECD_DB")
script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.') 
csv_path = os.path.join(script_dir, 'DATA_CSV')
os.chdir(csv_path)
extension = "csv"
all_filenames = [i for i in glob.glob('*{}'.format(extension))]
mydb = mysql.connector.connect(host = "localhost", user = 'root', passwd = '', database='OECD_DB', port="3306")
cursor = mydb.cursor()
print(len(all_filenames))
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
        
        i = 1
        while 1:
            createQueryString = ""
            valFormat = ""
            fieldName = ""
            if len(titles) < 1000:
                index = ""
            else:
                index = "("+str(i)+")"
            tname =  filename.replace('.csv', '')[:63].lower()
            while tname[-1] == " ":
                tname = tname[:-1]
            insertQueryString = 'INSERT INTO `' + tname + index + '`('
            tit = titles[1000*(i-1):1000*i]
            if not tit:
                break
            for string in tit:
                createQueryString += "`" + str(string) + "`" + " TEXT,"
                valFormat += "%s,"
                fieldName += "`" + str(string) + "`" +", "
            
            valFormat = valFormat[:-1]
            fieldName = fieldName[:-2]
            createQueryString = createQueryString[:-1]
            insertQueryString = insertQueryString + fieldName +") " + "VALUES (" + valFormat + ")"
            
            createQueryString = "CREATE TABLE IF NOT EXISTS `" + tname + index + "`(" + createQueryString +") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
            
            cursor.execute("SET GLOBAL innodb_default_row_format='dynamic'")
            cursor.execute("SET SESSION innodb_strict_mode=OFF")
            cursor.execute(createQueryString)
            tran = [list(j) for j in zip(*datas)]
            x = [m[1000*(i-1):1000*i] for m in tran if m[1000*(i-1):1000*i]]
            result = []
            for j in x:
                result.append(tuple(j))
            j = 0
            while 1:
                if result[100*j:100*(j+1)]:
                    cursor.executemany(insertQueryString, result[100*j:100*(j+1)])
                else:
                    break
                j += 1
            mydb.commit()
            i += 1
    print("importing successfully finished")