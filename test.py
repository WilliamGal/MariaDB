from bs4 import BeautifulSoup
from requests import get
import pandas as pd
from sqlalchemy import create_engine
import mysql.connector

#Donnees stockees suite au check pour chaque commune
LinkFile = open("Liens.txt","r")
CityFile = open("Villes.txt","r")

def clean(str,bool):
    if str=='-':
        return None
    str=str.replace(' ',"")
    str=str.replace('\n',"")
    str=str.replace('\xa0',"")
    str=str.replace('&nbsp;',"")
    str=str.replace('%',"")
    if(bool):
        str=str.replace(',',".")
        str=str.replace('€',"")
        i=float(str)
        return i;
    return str;
    
def cleanPop(str):
    str=str.replace(' ',"")
    str=str.replace('\n',"")
    str=str.replace('\xa0',"")
    str=str.replace('&nbsp;',"")
    str=str.replace('habitants',"")
    i=int(str)
    return i
    
def cleanEconomy(str):
    str=str.replace(' ',"")
    str=str.replace('\n',"")
    str=str.replace('\xa0',"")
    str=str.replace('&nbsp;',"")
    str=str.replace('€',"")
        
def manageEcoData(x,i):
    try:
        resp=get(x)
        soup=BeautifulSoup(resp.text,'html.parser')
        infos=soup.find('div',class_="population")
        infos=infos.find_all('li')
        a=clean(infos[i].text.split(':')[1],True)
        return a;
    except:
        return None
    
        
l=[]
c=[]
cp=[]

AvPrice=[]
YearEvo=[]
MonthlyRent=[]
YearEvoRent=[]
Yield=[]

Pop=[]

UnemploymentRate=[] #DepartmentRate
TaxUnit=[]
NetIncome=[]
NetIncomeByHousehold=[]
PayWealthTaxes=[]
AvIncomeTax=[]

clines=CityFile.readlines()
for i in range(len(clines)):
    y=clines[i]
    y=y.replace('\n',"")
    e=y.split('-')
    n=len(e)

    if n>2:
        city=""
        for j in range(n-2):
            city+=e[j]+'-'
        city+=e[n-2]
        c.append(city)
        cp.append(e[n-1])
    else:
        c.append(e[0])
        cp.append(e[1])

index=0
lines=LinkFile.readlines()

for i in range(len(lines)):
    x=lines[i]
    x=x.replace('\n',"")
    resp=get(x)
    soup=BeautifulSoup(resp.text,'html.parser')
    infos=soup.find_all('tr')
    infos=infos[2].find_all('td')
    AvPrice.append(clean(infos[1].text,True))
    YearEvo.append(clean(infos[2].text,True))
    MonthlyRent.append(clean(infos[3].text,True))
    YearEvoRent.append(clean(infos[4].text,True))
    Yield.append(clean(infos[5].text,True))
    
    x=x.replace('prix','population-typologie')
    resp=get(x)
    soup=BeautifulSoup(resp.text,'html.parser')
    infos=soup.find_all('tr')
    infos=infos[0].find_all('td')
    Pop.append(cleanPop(infos[1].text))
    
    x=x.replace('population-typologie','economie')
    UnemploymentRate.append(manageEcoData(x,3))
    TaxUnit.append(manageEcoData(x,4))
    NetIncome.append(manageEcoData(x,5))
    NetIncomeByHousehold.append(manageEcoData(x,6))
    PayWealthTaxes.append(manageEcoData(x,7))
    AvIncomeTax.append(manageEcoData(x,8))
    


LinkFile.close()

CityFile.close()

infos_df=pd.DataFrame({'City':c,'Postal Code':cp,'Habitation Average Price':AvPrice,'Percentage of Price Year Evolution':YearEvo,'Monthly Rent':MonthlyRent,'Net Income By Household':NetIncomeByHousehold,'Number Of Tax Unit':TaxUnit,'Percentage of Rent Year Evolution':YearEvoRent,'Yield':Yield,'Population':Pop,'Unemployment Rate':UnemploymentRate,'Total Net Income':NetIncome,'Net Income By Household':NetIncomeByHousehold,'Average Income Tax':AvIncomeTax,'Percentage Who Pay Wealth Taxes':PayWealthTaxes})

engine = create_engine('mysql+mysqlconnector://test:Sy8jutr6@test.cz4kv1pmytby.eu-west-3.rds.amazonaws.com:3306/test', echo=False)

infos_df.to_sql(name='test', con=engine, if_exists = 'replace', index=False)

print("Execute ! No error")
