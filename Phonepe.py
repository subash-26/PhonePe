import os
import json
import pandas as pd
import numpy as np

import pymysql
import mysql.connector
import streamlit as st
import plotly.express as px
from streamlit_option_menu import option_menu

import PIL
from PIL import Image

import geopandas as gpd
import matplotlib.pyplot as plt
import plotly as plot
from geopandas import plotting
import requests
import json
from sqlalchemy.engine import URL
from sqlalchemy import create_engine










mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="DatascienceProject098",
)

print(mydb)
mycursor = mydb.cursor(buffered=True)


alchemydb= create_engine('mysql+mysqlconnector://root:DatascienceProject098@localhost/Phonepe')


#Dashboard Configuratin
st.set_page_config(layout='wide', page_title='PhonePe Data Analysis')


#Transcation_Aggerate

Trans_Agg_Path="C:/Users/subash/OneDrive/Guvi/Project/PhonePe/pulse/data/aggregated/transaction/country/india/state/"

Trans_Agg_list= os.listdir(Trans_Agg_Path)

Table1={"States":[], "Years":[], "Quarter":[], "Transcation_type": [], "Transcation_count": [], "Transcation_amount":[]}

for state in Trans_Agg_list:
    states_details = Trans_Agg_Path+state+"/"
    Agg_Year_list = os.listdir(states_details) #convert the string into location path
    #print(Agg_Year_list)

    for year in Agg_Year_list:
        cur_year = states_details+year+"/"
        Agg_File_List = os.listdir(cur_year)
        #print(Agg_File_List)

        for file in Agg_File_List:
            cur_file = cur_year + file
            data = open(cur_file,"r")

            pri=json.load(data)

            for i in pri["data"]["transactionData"]:
                name=i["name"]
                count=i["paymentInstruments"][0]["count"]
                amount=i["paymentInstruments"][0]["amount"]
                Table1["Transcation_type"].append(name)
                Table1["Transcation_count"].append(count)
                Table1["Transcation_amount"].append(amount)
                Table1["States"].append(state)
                Table1["Years"].append(year)
                Table1["Quarter"].append(int(file.strip(".json")))

            
Table1

Agg_Transcation=pd.DataFrame(Table1)

User_Aggre_Path="C:/Users/subash/OneDrive/Guvi/Project/PhonePe/pulse/data/aggregated/user/country/india/state/"

User_Aggre_List=os.listdir(User_Aggre_Path)

Table2={"States":[],"Year":[],"Quarter":[],"Brand":[],"Transcation_count":[],"Percentage":[]}

for state in User_Aggre_List:
    states_details = User_Aggre_Path+state+"/"
    Agg_Year_list = os.listdir(states_details) #convert the string into location path
    #print(Agg_Year_list)

    for year in Agg_Year_list:
        cur_year = states_details+year+"/"
        Agg_File_List = os.listdir(cur_year)
        #print(Agg_File_List)

        for file in Agg_File_List:
            cur_file = cur_year + file
            data = open(cur_file,"r")

            useaggpth=json.load(data)
            print(useaggpth)

            try:
               
                for i in useaggpth["data"]["usersByDevice"]:
                    brand=i["brand"]
                    count=i["count"]
                    percentage=i["percentage"]
                    Table2["Brand"].append(brand)
                    Table2["Transcation_count"].append(count)
                    Table2["Percentage"].append(percentage)
                    Table2["States"].append(state)
                    Table2["Year"].append(year)
                    Table2["Quarter"].append(int(file.strip(".json")))

            except:
                pass 
            

User_aggerator_Transcation=pd.DataFrame(Table2)


#Map Transcation Path
Map_Transcation_Path = "C:/Users/subash/OneDrive/Guvi/Project/PhonePe/pulse/data/map/transaction/hover/country/india/state/"

Map_Transcation_List= os.listdir(Map_Transcation_Path)

Table3={"States":[],"Year":[],"Quarter":[],"District":[],"Transcation_count":[],"Transcation_amount":[]}

for state in Map_Transcation_List:
    states_details = Map_Transcation_Path+state+"/"
    Agg_Year_list = os.listdir(states_details) #convert the string into location path
    #print(Agg_Year_list)

    for year in Agg_Year_list:
        cur_year = states_details+year+"/"
        Agg_File_List = os.listdir(cur_year)
        #print(Agg_File_List)

        for file in Agg_File_List:
            cur_file = cur_year + file
            data = open(cur_file,"r")

            maptrnspth=json.load(data)

            for i in maptrnspth["data"]["hoverDataList"]:
                    name=i["name"]
                    count=i['metric'][0]['count']
                    amount=i['metric'][0]['amount']
                    Table3["District"].append(name)
                    Table3["Transcation_count"].append(count)
                    Table3["Transcation_amount"].append(amount)
                    Table3["States"].append(state)
                    Table3["Year"].append(year)
                    Table3["Quarter"].append(int(file.strip(".json")))

Map_Transcation_Aggera=pd.DataFrame(Table3)

print(Map_Transcation_Aggera)

#Map User Agger 

Map_User_Agger_Path="C:/Users/subash/OneDrive/Guvi/Project/PhonePe/pulse/data/map/user/hover/country/india/state/"

Map_User_Agger_List=os.listdir(Map_User_Agger_Path)

Table4={"States":[],"Year":[],"Quarter":[],"District":[],"Registered_Users":[],"App_Opens":[]}

for state in Map_User_Agger_List:
    states_details = Map_User_Agger_Path+state+"/"
    Agg_Year_list = os.listdir(states_details) #convert the string into location path
    #print(Agg_Year_list)
    
    for year in Agg_Year_list:
        cur_year = states_details+year+"/"
        Agg_File_List = os.listdir(cur_year)
        #print(Agg_File_List)

        for file in Agg_File_List:
            cur_file = cur_year + file
            data = open(cur_file,"r")

            Mapuseragg=json.load(data)

            for i in Mapuseragg["data"]["hoverData"].items():
                    district=i[0]
                    registeredUsers=i[1]["registeredUsers"]
                    appOpens=i[1]["appOpens"]
                    Table4["District"].append(district)
                    Table4["Registered_Users"].append(registeredUsers)
                    Table4["App_Opens"].append(appOpens)
                    Table4["States"].append(state)
                    Table4["Year"].append(year)
                    Table4["Quarter"].append(int(file.strip(".json")))

Map_User_Agger_Table=pd.DataFrame(Table4)

#Top Transcation Path
Top_Transcation_Path = "C:/Users/subash/OneDrive/Guvi/Project/PhonePe/pulse/data/top/transaction/country/india/state/"

Top_Transcation_List= os.listdir(Top_Transcation_Path)

Table5={"States":[],"Year":[],"Quarter":[],"District":[],"Transcation_count":[],"Transcation_amount":[]}

for state in Top_Transcation_List:
    states_details = Top_Transcation_Path+state+"/"
    Agg_Year_list = os.listdir(states_details) #convert the string into location path
    #print(Agg_Year_list)

    for year in Agg_Year_list:
        cur_year = states_details+year+"/"
        Agg_File_List = os.listdir(cur_year)
        #print(Agg_File_List)

        for file in Agg_File_List:
            cur_file = cur_year + file
            data = open(cur_file,"r")

            toptrnspth=json.load(data)

            for i in toptrnspth["data"]["districts"]:
                name=i["entityName"]
                count=i["metric"]["count"]
                amount=i["metric"]["amount"]
                Table5["District"].append(name)
                Table5["Transcation_count"].append(count)
                Table5["Transcation_amount"].append(amount)
                Table5["States"].append(state)
                Table5["Year"].append(year)
                Table5["Quarter"].append(int(file.strip(".json")))

Top_Transcation_List=pd.DataFrame(Table5)




#Top User Transcation
Top_User_Path = "C:/Users/subash/OneDrive/Guvi/Project/PhonePe/pulse/data/top/user/country/india/state/"

Top_User_Path_List=os.listdir(Top_User_Path)

Table61={"States":[],"Year":[],"Quarter":[],"Disname":[],"Dreguser":[]}

Table62={"Pincode_Name":[],"Pincode_Reguser":[]}

for state in Top_User_Path_List:
    states_details = Top_User_Path+state+"/"
    Agg_Year_list = os.listdir(states_details) #convert the string into location path
    #print(Agg_Year_list)

    for year in Agg_Year_list:
        cur_year = states_details+year+"/"
        Agg_File_List = os.listdir(cur_year)
        #print(Agg_File_List)

        for file in Agg_File_List:
            cur_file = cur_year + file
            data = open(cur_file,"r")

            topuserpth=json.load(data)

            #topuserdict=dict(topuserpth["data"])

            for i in topuserpth["data"]["districts"]:
                dname=i["name"]
                #print(dname)
                dreguser=i["registeredUsers"]
                #print(dreguser)
                Table61["Disname"].append(dname)
                Table61["Dreguser"].append(dreguser)
                Table61["States"].append(state)
                Table61["Year"].append(year)
                Table61["Quarter"].append(int(file.strip(".json")))

            for i in topuserpth["data"]["pincodes"]:
                pname=i["name"]
                preguser=i["registeredUsers"]
                Table62["Pincode_Name"].append(pname)
                Table62["Pincode_Reguser"].append(preguser)

Top_User_Table1=pd.DataFrame(Table61)

Top_User_Table2=pd.DataFrame(Table62)

Top_User_Table=Top_User_Table1.join(Top_User_Table2)