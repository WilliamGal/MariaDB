from bs4 import BeautifulSoup
from requests import get
import mysql.connector

url = 'https://fr.finance.yahoo.com/devisas'

response = get(url)
html_soup = BeautifulSoup(response.text, 'html.parser')
data=html_soup.find_all('tr')


FinalData=[]

def clean(str):
    if str=='-':
        return None
    str=str.replace(' ',"")
    str=str.replace('\xa0',"")
    str=str.replace('%',"")
    str=str.replace(',',".")
    str=str.replace('€',"")
    return float(str);

for i in range(1,29):
    a=data[i].find_all('td')
    Symbol=a[0].text
    Name=a[1].text
    Quotation=clean(a[2].text)
    Var=clean(a[3].text)
    PercentVar=clean(a[4].text)
    FinalData.append((Name,Quotation,Var,PercentVar,Symbol))
    

connection = mysql.connector.connect(host='newdb.cz4kv1pmytby.eu-west-3.rds.amazonaws.com',
                        database='newDB',
                        user='newDB',
                        password='Sy8jutr6')
                        
#sql_drop = """ DROP TABLE IF EXISTS Devises """

#sql_create = """ CREATE TABLE Devises(
 #   Symbol VARCHAR(20),
  #  Name VARCHAR(20),
   # Quotation DOUBLE(40,4),
    #Var DOUBLE(40,4),
    #PercentVar DOUBLE(40,4)
#)
#WITH SYSTEM VERSIONING;
#"""                        


sql_insert_query = """ UPDATE Devises SET Name = %s , Quotation =%s , Var=%s , PercentVar=%s WHERE Symbol=%s; """


cursor = connection.cursor(prepared=True)
#used executemany to insert 3 rows
#result0  = cursor.execute(sql_drop)
#result1  = cursor.execute(sql_create)
result2  = cursor.executemany(sql_insert_query,FinalData)
connection.commit()


print("Currencies updated")
