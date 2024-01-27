import requests
import pandas as pd
import numpy as np
import json
import pymysql
import mysql.connector
import streamlit as st
import plotly.express as px
from plotly.express import choropleth

import PIL
from PIL import Image
from streamlit_option_menu import option_menu
from sqlalchemy.engine import URL
from sqlalchemy import create_engine


mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="DatascienceProject098",
  database="Phonepe"
)

print(mydb)
cursor = mydb.cursor(buffered=True)

alchemydb= create_engine('mysql+mysqlconnector://root:DatascienceProject098@localhost/Phonepe')

st.set_page_config(layout='wide', page_title='PhonePe Data Analysis')

page_be_image = f"""
<style>
[data-testid="stAppViewContainer"]> .main{{
background-color: #87CEFA;
background-size: cover;
background-position: top left;
background-repeat: no-repeat;
}}

[data-testid="stHeader"] {{
background-color: #87CEFA;
}}
</style>
"""

st.markdown(page_be_image,unsafe_allow_html=True)

selected = option_menu('Phonepe Data Visualization',
                       options = ["About","Analysis","Insights",],
                       icons = ["bar-chart","toggles","at"],
                       default_index=0,
                       orientation="horizontal",
                       styles={"container": {"width": "100%"},
                               "icon": {"color": "violet", "font-size": "24px"},
                               "nav-link": {"font-size": "24px","color":"#007bff", "text-align": "center", "margin": "-2px"},
                               "nav-link-selected": {"background-color": "#6F36AD"}})





# ABOUT TAB
if selected == "About":
    col1, col2, = st.columns(2)
    col2.image(Image.open(r"C:\Users\subash\OneDrive\Guvi\Project\PhonePe Codeing\phonepeicon.jpg"), width=750,caption='A product from India')
    with col1:
        st.title(':violet[PhonePe]')
        st.divider()
        st.subheader(':blue[Founders : Sameer Nigam, Rahul Chari and Burzin Enginee]')
        st.write('PhonePe is an Indian digital payments and financial services company headquartered in Bengaluru, Karnataka, India.')
        st.write('PhonePe was founded in December 2015')
        st.write('The PhonePe app, based on the Unified Payments Interface (UPI), went live in August 2016.')
        st.write('The PhonePe app is available in 11 Indian languages. Using PhonePe, users can send and receive money, recharge mobile, DTH, data cards, make utility payments, pay at shops, invest in tax saving funds, buy insurance, mutual funds, and  digital gold')
        st.write(' PhonePe is accepted as a payment option by over 3.6 crore offline and online merchant outlets, constituting 99% of pin codes in the country. The app served more than 10 crore users as of June 2018, processed 500 crore transactions by December 2019, and crossed 10 crore transactions a day in April 2022. It currently has over 50 crore registered users with over 20 crore monthly active users PhonePe is licensed by the Reserve Bank of India for the issuance and operation of a Semi Closed Prepaid Payment system')
    with col2:
        st.markdown("[DOWNLOAD APP](https://www.phonepe.com/app-download/)",unsafe_allow_html=True)
    st.video(r"C:\Users\subash\Downloads\Phonepe.mp4")

# ANALYSIS TAB
if selected == "Analysis":
    st.title(':violet[ANALYSIS]')
    st.subheader('Analysis done on the basis of All India ,States and Top categories between 2018 and 2022')
    select = option_menu(None,
                         options=["INDIA", "STATES", "TOP CATEGORIES" ],
                         default_index=0,
                         orientation="horizontal",
                         styles={"container": {"width": "100%"},
                                   "nav-link": {"font-size": "20px", "text-align": "center", "margin": "-2px"},
                                   "nav-link-selected": {"background-color": "#6F36AD"}})
    if select == "INDIA":
        tab1, tab2 = st.tabs(["TRANSACTION","USER"])

        # TRANSACTION TAB
        with tab1:
            col1, col2, col3 = st.columns(3)
            with col1:
                in_tr_yr = st.selectbox('**Select Year**', ('2018', '2019', '2020', '2021', '2022'), key='in_tr_yr')
            with col2:
                in_tr_qtr = st.selectbox('**Select Quarter**', ('1', '2', '3', '4'), key='in_tr_qtr')
            with col3:
                in_tr_tr_typ = st.selectbox('**Select Transaction type**',
                                            ('Recharge & bill payments', 'Peer-to-peer payments',
                                             'Merchant payments', 'Financial Services', 'Others'), key='in_tr_tr_typ')
            # SQL Query

            # Transaction Analysis bar chart query
            cursor.execute(
                f"SELECT States, Transcation_amount FROM aggtranscation WHERE Years = '{in_tr_yr}' AND Quarter = '{in_tr_qtr}' AND Transcation_type = '{in_tr_tr_typ}';")
            in_tr_tab_qry_rslt = cursor.fetchall()
            df_in_tr_tab_qry_rslt = pd.DataFrame(np.array(in_tr_tab_qry_rslt), columns=['States', 'Transcation_amount'])
            df_in_tr_tab_qry_rslt1 = df_in_tr_tab_qry_rslt.set_index(pd.Index(range(1, len(df_in_tr_tab_qry_rslt) + 1)))

            # Transaction Analysis table query
            cursor.execute(
                f"SELECT States, Transcation_count, Transcation_amount FROM aggtranscation WHERE Years = '{in_tr_yr}' AND Quarter = '{in_tr_qtr}' AND Transcation_type = '{in_tr_tr_typ}';")
            in_tr_anly_tab_qry_rslt = cursor.fetchall()
            df_in_tr_anly_tab_qry_rslt = pd.DataFrame(np.array(in_tr_anly_tab_qry_rslt),
                                                      columns=['States', 'Transcation_count', 'Transcation_amount'])
            df_in_tr_anly_tab_qry_rslt1 = df_in_tr_anly_tab_qry_rslt.set_index(
                pd.Index(range(1, len(df_in_tr_anly_tab_qry_rslt) + 1)))

            # Total Transaction Amount table query
            cursor.execute(
                f"SELECT SUM(Transcation_amount), AVG(Transcation_amount) FROM aggtranscation WHERE Years = '{in_tr_yr}' AND Quarter = '{in_tr_qtr}' AND Transcation_type = '{in_tr_tr_typ}';")
            in_tr_am_qry_rslt = cursor.fetchall()
            df_in_tr_am_qry_rslt = pd.DataFrame(np.array(in_tr_am_qry_rslt), columns=['Total', 'Average'])
            df_in_tr_am_qry_rslt1 = df_in_tr_am_qry_rslt.set_index(['Average'])

            # Total Transaction Count table query
            cursor.execute(
                f"SELECT SUM(Transcation_count), AVG(Transcation_count) FROM aggtranscation WHERE Years = '{in_tr_yr}' AND Quarter = '{in_tr_qtr}' AND Transcation_type = '{in_tr_tr_typ}';")
            in_tr_co_qry_rslt = cursor.fetchall()
            df_in_tr_co_qry_rslt = pd.DataFrame(np.array(in_tr_co_qry_rslt), columns=['Total', 'Average'])
            df_in_tr_co_qry_rslt1 = df_in_tr_co_qry_rslt.set_index(['Average'])

            # GEO VISUALISATION
            # Drop a State column from df_in_tr_tab_qry_rslt
            df_in_tr_tab_qry_rslt.drop(columns=['States'], inplace=True)
            # Clone the gio data
            url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
            response = requests.get(url)
            data1 = json.loads(response.content)
            # Extract state names and sort them in alphabetical order
            state_names_tra = [feature['properties']['ST_NM'] for feature in data1['features']]
            state_names_tra.sort()
            # Create a DataFrame with the state names column
            df_state_names_tra = pd.DataFrame({'States': state_names_tra})
            # Combine the Gio State name with df_in_tr_tab_qry_rslt
            df_state_names_tra['Transcation_amount'] = df_in_tr_tab_qry_rslt
            # convert dataframe to csv file
            df_state_names_tra.to_csv('State_trans.csv', index=False)
            # Read csv
            df_tra = pd.read_csv('State_trans.csv')
            # Geo plot
            
            fig_tra = px.choropleth(
                df_tra,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM', locations='States', color='Transcation_amount',
                color_continuous_scale='rainbow', title='Transaction Analysis')
            fig_tra.update_geos(fitbounds="locations", visible=False)
            fig_tra.update_layout(title_font=dict(size=33),paper_bgcolor='#6529B7',
                                  geo=dict(
                                                bgcolor='#6529B7',    # Background color for the entire map area
                                                showland=False,        # Show land outlines
                                                showcoastlines=False,  # Show coastlines
                                                showframe=False,      # Hide frame
                                                projection_type='equirectangular'  # Projection type
                                            ),plot_bgcolor='#6529B7',hovermode="closest", height=1000)
            st.plotly_chart(fig_tra, use_container_width=True)

            # ---------   /   All India Transaction Analysis Bar chart  /  ----- #
            df_in_tr_tab_qry_rslt1['States'] = df_in_tr_tab_qry_rslt1['States'].astype(str)
            df_in_tr_tab_qry_rslt1['Transcation_amount'] = df_in_tr_tab_qry_rslt1['Transcation_amount'].astype(float)
            df_in_tr_tab_qry_rslt1_fig = px.bar(df_in_tr_tab_qry_rslt1, x='States', y='Transcation_amount',
                                                color='Transcation_amount', color_continuous_scale='thermal',
                                                title='Transaction Analysis Chart', height=700, )
            df_in_tr_tab_qry_rslt1_fig.update_layout(title_font=dict(size=33),paper_bgcolor='#87CEFA', 
                                  geo=dict(
                                                bgcolor='#87CEFA',    # Background color for the entire map area
                                                showland=False,        # Show land outlines
                                                showcoastlines=False,  # Show coastlines
                                                showframe=False,      # Hide frame
                                                projection_type='equirectangular'  # Projection type
                                            ),plot_bgcolor='#87CEFA')
            st.plotly_chart(df_in_tr_tab_qry_rslt1_fig, use_container_width=True)

            # -------  /  All India Total Transaction calculation Table   /   ----  #
            st.header(':violet[Total calculation]')

            col4, col5 = st.columns(2)
            with col4:
                st.subheader(':violet[Transaction Analysis]')
                st.dataframe(df_in_tr_anly_tab_qry_rslt1)
            with col5:
                st.subheader(':violet[Transaction Amount]')
                st.dataframe(df_in_tr_am_qry_rslt1)
                st.subheader(':violet[Transaction Count]')
                st.dataframe(df_in_tr_co_qry_rslt1)

        # USER TAB
        with tab2:
            col1, col2 = st.columns(2)
            with col1:
                in_us_yr = st.selectbox('**Select Year**', ('2018', '2019', '2020', '2021', '2022'), key='in_us_yr')
            with col2:
                in_us_qtr = st.selectbox('**Select Quarter**', ('1', '2', '3', '4'), key='in_us_qtr')

            # SQL Query

            # User Analysis Bar chart query
            cursor.execute(f"SELECT States, SUM(Transcation_count) FROM useraggtranscation WHERE Year = '{in_us_yr}' AND Quarter = '{in_us_qtr}' GROUP BY States;")
            in_us_tab_qry_rslt = cursor.fetchall()
            df_in_us_tab_qry_rslt = pd.DataFrame(np.array(in_us_tab_qry_rslt), columns=['States', 'Transcation_count'])
            df_in_us_tab_qry_rslt1 = df_in_us_tab_qry_rslt.set_index(pd.Index(range(1, len(df_in_us_tab_qry_rslt) + 1)))

            # Total User Count table query
            cursor.execute(f"SELECT SUM(Transcation_count), AVG(Transcation_count) FROM useraggtranscation WHERE Year = '{in_us_yr}' AND Quarter = '{in_us_qtr}'GROUP BY States;")
            in_us_co_qry_rslt = cursor.fetchall()
            df_in_us_co_qry_rslt = pd.DataFrame(np.array(in_us_co_qry_rslt), columns=['Total', 'Average'])
            df_in_us_co_qry_rslt1 = df_in_us_co_qry_rslt.set_index(['Average'])



            # GEO VISUALIZATION FOR USER

            # Drop a State column from df_in_us_tab_qry_rslt
            df_in_us_tab_qry_rslt.drop(columns=['States'], inplace=True)
            # Clone the gio data
            url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
            response = requests.get(url)
            data2 = json.loads(response.content)
            # Extract state names and sort them in alphabetical order
            state_names_use = [feature['properties']['ST_NM'] for feature in data2['features']]
            state_names_use.sort()
            # Create a DataFrame with the state names column
            df_state_names_use = pd.DataFrame({'States': state_names_use})
            # Combine the Gio State name with df_in_tr_tab_qry_rslt
            df_state_names_use['Transcation_count'] = df_in_us_tab_qry_rslt
            # convert dataframe to csv file
            df_state_names_use.to_csv('State_user.csv', index=False)
            # Read csv
            df_use = pd.read_csv('State_user.csv')
            # Geo plot
            fig_use = px.choropleth(
                df_use,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM', locations='States', color='Transcation_count',
                color_continuous_scale='rainbow', title='User Analysis')
            fig_use.update_geos(fitbounds="locations", visible=False)
            fig_use.update_layout(title_font=dict(size=33),paper_bgcolor='#6529B7', 
                                  geo=dict(
                                                bgcolor='#6529B7',    # Background color for the entire map area
                                                showland=False,        # Show land outlines
                                                showcoastlines=False,  # Show coastlines
                                                showframe=False,      # Hide frame
                                                projection_type='equirectangular'  # Projection type
                                            ),plot_bgcolor='#6529B7',hovermode="closest", height=1000)
            st.plotly_chart(fig_use, use_container_width=True)

            # ----   /   All India User Analysis Bar chart   /     -------- #
            df_in_us_tab_qry_rslt1['States'] = df_in_us_tab_qry_rslt1['States'].astype(str)
            df_in_us_tab_qry_rslt1['Transcation_count'] = df_in_us_tab_qry_rslt1['Transcation_count'].astype(int)
            df_in_us_tab_qry_rslt1_fig = px.bar(df_in_us_tab_qry_rslt1, x='States', y='Transcation_count', color='Transcation_count',
                                                color_continuous_scale='thermal', title='User Analysis Chart',
                                                height=700, )
            df_in_us_tab_qry_rslt1_fig.update_layout(title_font=dict(size=33),paper_bgcolor='#87CEFA', 
					  geo=dict(
									bgcolor='#87CEFA',    # Background color for the entire map area
									showland=False,        # Show land outlines
									showcoastlines=False,  # Show coastlines
									showframe=False,      # Hide frame
									projection_type='equirectangular'  # Projection type
								),plot_bgcolor='#87CEFA')
            st.plotly_chart(df_in_us_tab_qry_rslt1_fig, use_container_width=True)

            # -----   /   All India Total User calculation Table   /   ----- #
            st.header(':violet[Total calculation]')

            col3, col4 = st.columns(2)
            with col3:
                st.subheader(':violet[User Analysis]')
                st.dataframe(df_in_us_tab_qry_rslt1)
            with col4:
                st.subheader(':violet[User Count]')
                st.dataframe(df_in_us_co_qry_rslt1)

    # STATE TAB
    if select == "STATES":
        tab3 ,tab4 = st.tabs(["TRANSACTION","USER"])

        #TRANSACTION TAB FOR STATE
        with tab3:
            col1, col2, col3 = st.columns(3)
            with col1:
                st_tr_st = st.selectbox('**Select State**', (
                'Andaman & Nicobar', 'Andhra Pradesh', 'Arunachal Pradesh', 'Assam', 'Bihar',
                'Chandigarh', 'Chhattisgarh', 'Dadra and Nagar Haveli and Daman and Diu', 'Delhi', 'Goa', 'Gujarat',
                'Haryana', 'Himachal Pradesh',
                'Jammu & Kashmir', 'Jharkhand', 'Karnataka', 'Kerala', 'Ladakh', 'Lakshadweep', 'Madhya Pradesh',
                'Maharashtra', 'Manipur',
                'Meghalaya', 'Mizoram', 'Nagaland', 'Odisha', 'Puducherry', 'Punjab', 'Rajasthan', 'Sikkim',
                'Tamil Nadu', 'Telangana',
                'Tripura', 'Uttar Pradesh', 'Uttarakhand', 'West Bengal'), key='st_tr_st')
            with col2:
                st_tr_yr = st.selectbox('**Select Year**', ('2018', '2019', '2020', '2021', '2022'), key='st_tr_yr')
            with col3:
                st_tr_qtr = st.selectbox('**Select Quarter**', ('1', '2', '3', '4'), key='st_tr_qtr')


            # SQL QUERY

            #Transaction Analysis bar chart query
            cursor.execute(f"SELECT Transcation_type, Transcation_amount FROM aggtranscation WHERE States = '{st_tr_st}' AND Years = '{st_tr_yr}' AND Quarter = '{st_tr_qtr}';")
            st_tr_tab_bar_qry_rslt = cursor.fetchall()
            df_st_tr_tab_bar_qry_rslt = pd.DataFrame(np.array(st_tr_tab_bar_qry_rslt),
                                                     columns=['Transcation_type', 'Transcation_amount'])
            df_st_tr_tab_bar_qry_rslt1 = df_st_tr_tab_bar_qry_rslt.set_index(
                pd.Index(range(1, len(df_st_tr_tab_bar_qry_rslt) + 1)))

            # Transaction Analysis table query
            cursor.execute(f"SELECT Transcation_type, Transcation_count, Transcation_amount FROM aggtranscation WHERE States = '{st_tr_st}' AND Years = '{st_tr_yr}' AND Quarter = '{st_tr_qtr}';")
            st_tr_anly_tab_qry_rslt = cursor.fetchall()
            df_st_tr_anly_tab_qry_rslt = pd.DataFrame(np.array(st_tr_anly_tab_qry_rslt),
                                                      columns=['Transcation_type', 'Transcation_count',
                                                               'Transcation_amount'])
            df_st_tr_anly_tab_qry_rslt1 = df_st_tr_anly_tab_qry_rslt.set_index(
                pd.Index(range(1, len(df_st_tr_anly_tab_qry_rslt) + 1)))

            # Total Transaction Amount table query
            cursor.execute(f"SELECT SUM(Transcation_amount), AVG(Transcation_amount) FROM aggtranscation WHERE States = '{st_tr_st}' AND Years = '{st_tr_yr}' AND Quarter = '{st_tr_qtr}';")
            st_tr_am_qry_rslt = cursor.fetchall()
            df_st_tr_am_qry_rslt = pd.DataFrame(np.array(st_tr_am_qry_rslt), columns=['Total', 'Average'])
            df_st_tr_am_qry_rslt1 = df_st_tr_am_qry_rslt.set_index(['Average'])

            # Total Transaction Count table query
            cursor.execute(f"SELECT SUM(Transcation_count), AVG(Transcation_count) FROM aggtranscation WHERE States = '{st_tr_st}' AND Years ='{st_tr_yr}' AND Quarter = '{st_tr_qtr}';")
            st_tr_co_qry_rslt = cursor.fetchall()
            df_st_tr_co_qry_rslt = pd.DataFrame(np.array(st_tr_co_qry_rslt), columns=['Total', 'Average'])
            df_st_tr_co_qry_rslt1 = df_st_tr_co_qry_rslt.set_index(['Average'])



            # -----    /   State wise Transaction Analysis bar chart   /   ------ #

            df_st_tr_tab_bar_qry_rslt1['Transcation_type'] = df_st_tr_tab_bar_qry_rslt1['Transcation_type'].astype(str)
            df_st_tr_tab_bar_qry_rslt1['Transcation_amount'] = df_st_tr_tab_bar_qry_rslt1['Transcation_amount'].astype(float)
            df_st_tr_tab_bar_qry_rslt1_fig = px.bar(df_st_tr_tab_bar_qry_rslt1, x='Transcation_type',
                                                    y='Transcation_amount', color='Transcation_amount',
                                                    color_continuous_scale='thermal',
                                                    title='Transaction Analysis Chart', height=500, )
            df_st_tr_tab_bar_qry_rslt1_fig.update_layout(title_font=dict(size=33),paper_bgcolor='#87CEFA', 
					  geo=dict(
									bgcolor='#87CEFA',    # Background color for the entire map area
									showland=False,        # Show land outlines
									showcoastlines=False,  # Show coastlines
									showframe=False,      # Hide frame
									projection_type='equirectangular'  # Projection type
								),plot_bgcolor='#87CEFA')
            st.plotly_chart(df_st_tr_tab_bar_qry_rslt1_fig, use_container_width=True)

            # ------  /  State wise Total Transaction calculation Table  /  ---- #
            st.header(':violet[Total calculation]')

            col4, col5 = st.columns(2)
            with col4:
                st.subheader(':violet[Transaction Analysis]')
                st.dataframe(df_st_tr_anly_tab_qry_rslt1)
            with col5:
                st.subheader(':violet[Transaction Amount]')
                st.dataframe(df_st_tr_am_qry_rslt1)
                st.subheader(':violet[Transaction Count]')
                st.dataframe(df_st_tr_co_qry_rslt1)


        # USER TAB FOR STATE
        with tab4:
            col5, col6 = st.columns(2)
            with col5:
                st_us_st = st.selectbox('**Select State**', (
                'Andaman & Nicobar', 'Andhra Pradesh', 'Arunachal Pradesh', 'Assam', 'Bihar',
                'Chandigarh', 'Chhattisgarh', 'Dadra and Nagar Haveli and Daman and Diu', 'Delhi', 'Goa', 'Gujarat',
                'Haryana', 'Himachal Pradesh',
                'Jammu & Kashmir', 'Jharkhand', 'Karnataka', 'Kerala', 'Ladakh', 'Lakshadweep', 'Madhya Pradesh',
                'Maharashtra', 'Manipur',
                'Meghalaya', 'Mizoram', 'Nagaland', 'Odisha', 'Puducherry', 'Punjab', 'Rajasthan', 'Sikkim',
                'Tamil Nadu', 'Telangana',
                'Tripura', 'Uttar Pradesh', 'Uttarakhand', 'West Bengal'), key='st_us_st')
            with col6:
                st_us_yr = st.selectbox('**Select Year**', ('2018', '2019', '2020', '2021', '2022'), key='st_us_yr')
            # SQL QUERY

            # User Analysis Bar chart query
            cursor.execute(f"SELECT Quarter, SUM(Transcation_count) FROM useraggtranscation WHERE States = '{st_us_st}' AND Year = '{st_us_yr}' GROUP BY Quarter;")
            st_us_tab_qry_rslt = cursor.fetchall()
            df_st_us_tab_qry_rslt = pd.DataFrame(np.array(st_us_tab_qry_rslt), columns=['Quarter', 'Transcation_count'])
            df_st_us_tab_qry_rslt1 = df_st_us_tab_qry_rslt.set_index(pd.Index(range(1, len(df_st_us_tab_qry_rslt) + 1)))

            # Total User Count table query
            cursor.execute(f"SELECT SUM(Transcation_count), AVG(Transcation_count) FROM useraggtranscation WHERE States = '{st_us_st}' AND Year = '{st_us_yr}';")
            st_us_co_qry_rslt = cursor.fetchall()
            df_st_us_co_qry_rslt = pd.DataFrame(np.array(st_us_co_qry_rslt), columns=['Total', 'Average'])
            df_st_us_co_qry_rslt1 = df_st_us_co_qry_rslt.set_index(['Average'])


            # -----   /   All India User Analysis Bar chart   /   ----- #
            df_st_us_tab_qry_rslt1['Quarter'] = df_st_us_tab_qry_rslt1['Quarter'].astype(int)
            df_st_us_tab_qry_rslt1['Transcation_count'] = df_st_us_tab_qry_rslt1['Transcation_count'].astype(int)
            df_st_us_tab_qry_rslt1_fig = px.bar(df_st_us_tab_qry_rslt1, x='Quarter', y='Transcation_count', color='Transcation_count',
                                                color_continuous_scale='thermal', title='User Analysis Chart',
                                                height=500, )
            df_st_us_tab_qry_rslt1_fig.update_layout(title_font=dict(size=33),paper_bgcolor='#87CEFA', 
					  geo=dict(
									bgcolor='#87CEFA',    # Background color for the entire map area
									showland=False,        # Show land outlines
									showcoastlines=False,  # Show coastlines
									showframe=False,      # Hide frame
									projection_type='equirectangular'  # Projection type
								),plot_bgcolor='#87CEFA')
            st.plotly_chart(df_st_us_tab_qry_rslt1_fig, use_container_width=True)

            # ------    /   State wise User Total User calculation Table   /   -----#
            st.header('Total calculation')

            col3, col4 = st.columns(2)
            with col3:
                st.subheader('User Analysis')
                st.dataframe(df_st_us_tab_qry_rslt1)
            with col4:
                st.subheader('Transcation_count')
                st.dataframe(df_st_us_co_qry_rslt1)

    # TOP CATEGORIES
    if select == "TOP CATEGORIES":
        tab5, tab6 = st.tabs(["TRANSACTION", "USER"])

        # Overall top transaction
        #TRANSACTION TAB
        with tab5:
            top_tr_yr = st.selectbox('**Select Year**', ('2018', '2019', '2020', '2021', '2022'), key='top_tr_yr')

            #SQL QUERY

            #Top Transaction Analysis bar chart query
            cursor.execute(
                f"SELECT States, SUM(Transcation_amount) As Transcation_amount FROM toptranscation WHERE Year = '{top_tr_yr}' GROUP BY States ORDER BY Transcation_amount DESC LIMIT 10;")
            top_tr_tab_qry_rslt = cursor.fetchall()
            df_top_tr_tab_qry_rslt = pd.DataFrame(np.array(top_tr_tab_qry_rslt),
                                                  columns=['States', 'Transcation_amount'])
            df_top_tr_tab_qry_rslt1 = df_top_tr_tab_qry_rslt.set_index(
                pd.Index(range(1, len(df_top_tr_tab_qry_rslt) + 1)))

            # Top Transaction Analysis table query
            cursor.execute(
                f"SELECT States, SUM(Transcation_amount) as Transcation_amount, SUM(Transcation_count) as Transcation_count FROM toptranscation WHERE Year = '{top_tr_yr}' GROUP BY States ORDER BY Transcation_amount DESC LIMIT 10;")
            top_tr_anly_tab_qry_rslt = cursor.fetchall()
            df_top_tr_anly_tab_qry_rslt = pd.DataFrame(np.array(top_tr_anly_tab_qry_rslt),
                                                       columns=['States', 'Transcation_amount',
                                                                'Transcation_count'])
            df_top_tr_anly_tab_qry_rslt1 = df_top_tr_anly_tab_qry_rslt.set_index(
                pd.Index(range(1, len(df_top_tr_anly_tab_qry_rslt) + 1)))



            # All India Transaction Analysis Bar chart
            df_top_tr_tab_qry_rslt1['States'] = df_top_tr_tab_qry_rslt1['States'].astype(str)
            df_top_tr_tab_qry_rslt1['Transcation_amount'] = df_top_tr_tab_qry_rslt1[
                'Transcation_amount'].astype(float)
            df_top_tr_tab_qry_rslt1_fig = px.bar(df_top_tr_tab_qry_rslt1, x='States', y='Transcation_amount',
                                                 color='Transcation_amount', color_continuous_scale='thermal',
                                                 title='Top Transaction Analysis Chart', height=600, )
            df_top_tr_tab_qry_rslt1_fig.update_layout(title_font=dict(size=33),paper_bgcolor='#87CEFA', 
					  geo=dict(
									bgcolor='#87CEFA',    # Background color for the entire map area
									showland=False,        # Show land outlines
									showcoastlines=False,  # Show coastlines
									showframe=False,      # Hide frame
									projection_type='equirectangular'  # Projection type
								),plot_bgcolor='#87CEFA')
            st.plotly_chart(df_top_tr_tab_qry_rslt1_fig, use_container_width=True)


            #All India Total Transaction calculation Table
            st.header('Total calculation')
            st.subheader('Top Transaction Analysis')
            st.dataframe(df_top_tr_anly_tab_qry_rslt1)

        # OVERALL TOP USER DATA
        # USER TAB
        with tab6:
            top_us_yr = st.selectbox('**Select Year**', ('2018', '2019', '2020', '2021', '2022'), key='top_us_yr')

            #SQL QUERY

            #Top User Analysis bar chart query
            cursor.execute(f"SELECT States, SUM(Dreguser) AS Top_user FROM topusertranscation WHERE Year='{top_us_yr}' GROUP BY States ORDER BY Top_user DESC LIMIT 10;")
            top_us_tab_qry_rslt = cursor.fetchall()
            df_top_us_tab_qry_rslt = pd.DataFrame(np.array(top_us_tab_qry_rslt), columns=['States', 'Total User count'])
            df_top_us_tab_qry_rslt1 = df_top_us_tab_qry_rslt.set_index(
                pd.Index(range(1, len(df_top_us_tab_qry_rslt) + 1)))



            #All India User Analysis Bar chart
            df_top_us_tab_qry_rslt1['States'] = df_top_us_tab_qry_rslt1['States'].astype(str)
            df_top_us_tab_qry_rslt1['Total User count'] = df_top_us_tab_qry_rslt1['Total User count'].astype(float)
            df_top_us_tab_qry_rslt1_fig = px.bar(df_top_us_tab_qry_rslt1, x='States', y='Total User count',
                                                 color='Total User count', color_continuous_scale='thermal',
                                                 title='Top User Analysis Chart', height=600, )
            df_top_us_tab_qry_rslt1_fig.update_layout(title_font=dict(size=33),paper_bgcolor='#87CEFA', 
					  geo=dict(
									bgcolor='#87CEFA',    # Background color for the entire map area
									showland=False,        # Show land outlines
									showcoastlines=False,  # Show coastlines
									showframe=False,      # Hide frame
									projection_type='equirectangular'  # Projection type
								),plot_bgcolor='#87CEFA')
            st.plotly_chart(df_top_us_tab_qry_rslt1_fig, use_container_width=True)

            #All India Total Transaction calculation Table
            st.header(':violet[Total calculation]')
            st.subheader('violet[Total User Analysis]')
            st.dataframe(df_top_us_tab_qry_rslt1)

#INSIGHTS TAB
if selected == "Insights":
    st.title(':violet[Data Visualization]')
    st.subheader("The basic insights are derived from the Analysis of the Phonepe Pulse data. It provides a clear idea about the analysed data.")
    options = ["--select--",
               "Top 10 states based on year and amount of transaction",
               "Least 10 states based on year and amount of transaction",
               "Top 10 States and Districts based on Registered Users",
               "Least 10 States and Districts based on Registered Users",
               "Top 10 Districts based on the Transaction Amount",
               "Least 10 Districts based on the Transaction Amount",
               "Top 10 Districts based on the Transaction count",
               "Least 10 Districts based on the Transaction count",
               "Top Transaction types based on the Transaction Amount",
               "Top 10 Mobile Brands based on the User count of transaction"]
    select = st.selectbox(":violet[Select the option]",options)

    #1
    if select == "Top 10 states based on year and amount of transaction":
        cursor.execute(
            "SELECT DISTINCT States,Year, SUM(Transcation_amount) AS Transcation_amount FROM toptranscation GROUP BY States,Year ORDER BY Transcation_amount DESC LIMIT 10");

        data = cursor.fetchall()
        columns = ['States', 'Year', 'Transcation_amount']
        df = pd.DataFrame(data, columns=columns, index=range(1, len(data) + 1))

        col1, col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Top 10 states based on amount of transaction")
            st.bar_chart(data=df, x="Transcation_amount", y="States")

    #2
    elif select == "Least 10 states based on year and amount of transaction":
        cursor.execute(
            "SELECT DISTINCT States,Year, SUM(Transcation_amount) as Total FROM toptranscation GROUP BY States, Year ORDER BY Total ASC LIMIT 10");
        data = cursor.fetchall()
        columns = ['States', 'Year', 'Transaction_amount']
        df = pd.DataFrame(data, columns=columns, index=range(1,len(data)+1))
        col1, col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Least 10 states based on amount of transaction")
            st.bar_chart(data=df, x="Transaction_amount", y="States")

    #3
    elif select == "Top 10 States and Districts based on Registered Users":
        cursor.execute("SELECT DISTINCT States, Pincode_Name, SUM(Pincode_Reguser) AS Users FROM topusertranscation GROUP BY States, Pincode_Name ORDER BY Users DESC LIMIT 10");
        data = cursor.fetchall()
        columns = ['States', 'District_Pincode', 'Registered_User']
        df = pd.DataFrame(data, columns=columns, index=range(1, len(data) + 1))

        col1, col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Top 10 States and Districts based on Registered Users")
            st.bar_chart(data=df, x="Registered_User", y="States")

    #4
    elif select == "Least 10 States and Districts based on Registered Users":
        cursor.execute("SELECT DISTINCT States, Pincode_Name, SUM(Pincode_Reguser) AS Users FROM topusertranscation GROUP BY States, Pincode_Name ORDER BY Users ASC LIMIT 10");
        data = cursor.fetchall()
        columns = ['State', 'District_Pincode', 'Registered_User']
        df = pd.DataFrame(data, columns=columns, index=range(1, len(data) + 1))
        col1, col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Least 10 States and Districts based on Registered Users")
            st.bar_chart(data=df, x="Registered_User", y="State")

    #5
    elif select == "Top 10 Districts based on the Transaction Amount":
        cursor.execute(
            "SELECT DISTINCT States ,District,SUM(Transcation_amount) AS Total FROM mapaggtranscation GROUP BY States ,District ORDER BY Total DESC LIMIT 10");
        data = cursor.fetchall()
        columns = ['States', 'District', 'Transaction_Amount']
        df = pd.DataFrame(data, columns=columns, index=range(1, len(data) + 1))
        col1, col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Top 10 Districts based on Transaction Amount")
            st.bar_chart(data=df, x="District", y="Transaction_Amount")

    #6
    elif select == "Least 10 Districts based on the Transaction Amount":
        cursor.execute(
            "SELECT DISTINCT States,District,SUM(Transcation_amount) AS Total FROM mapaggtranscation GROUP BY States, District ORDER BY Total ASC LIMIT 10");
        data = cursor.fetchall()
        columns = ['States', 'District', 'Transcation_amount']
        df = pd.DataFrame(data, columns=columns, index=range(1, len(data) + 1))

        col1, col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Least 10 Districts based on Transaction Amount")
            st.bar_chart(data=df, x="District", y="Transcation_amount")

    #7
    elif select == "Top 10 Districts based on the Transaction count":
        cursor.execute(
            "SELECT DISTINCT States,District,SUM(Transcation_count) AS Counts FROM mapaggtranscation GROUP BY States ,District ORDER BY Counts DESC LIMIT 10");
        data = cursor.fetchall()
        columns = ['States', 'District', 'Transcation_count']
        df = pd.DataFrame(data, columns=columns, index=range(1, len(data) + 1))

        col1, col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Top 10 Districts based on Transaction Count")
            st.bar_chart(data=df, x="Transcation_count", y="District")

    #8
    elif select == "Least 10 Districts based on the Transaction count":
        cursor.execute(
            "SELECT DISTINCT States ,District,SUM(Transcation_count) AS Counts FROM mapaggtranscation GROUP BY States ,District ORDER BY Counts ASC LIMIT 10");
        data = cursor.fetchall()
        columns = ['States', 'District', 'Transaction_Count']
        df = pd.DataFrame(data, columns=columns, index=range(1, len(data) + 1))

        col1, col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Top 10 Districts based on the Transaction Count")
            st.bar_chart(data=df, x="Transaction_Count", y="District")

    #9
    elif select == "Top Transaction types based on the Transaction Amount":
        cursor.execute(
            "SELECT DISTINCT Transcation_type, SUM(Transcation_amount) AS Amount FROM aggtranscation GROUP BY Transcation_type ORDER BY Amount DESC LIMIT 5");
        data = cursor.fetchall()
        columns = ['Transaction_type', 'Transaction_amount']
        df = pd.DataFrame(data, columns=columns, index=range(1, len(data) + 1))

        col1, col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Top Transaction Types based on the Transaction Amount")
            st.bar_chart(data=df, x="Transaction_type", y="Transaction_amount")

    #10
    elif select == "Top 10 Mobile Brands based on the User count of transaction":
        cursor.execute(
            "SELECT DISTINCT Brand,SUM(Transcation_count) as Total FROM useraggtranscation GROUP BY Brand ORDER BY Total DESC LIMIT 10");
        data = cursor.fetchall()
        columns = ['Brands', 'User_Count']
        df = pd.DataFrame(data, columns=columns, index=range(1, len(data) + 1))

        col1, col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            st.title("Top 10 Mobile Brands based on User count of transaction")
            st.bar_chart(data=df , x="User_Count", y="Brands")

    #DOWNLOAD REPORT
    st.subheader(":violet[The Annual Report of Phonepe Pulse data]")
    with open(r"C:\Users\subash\Downloads\PhonePe_Pulse_BCG_report.pdf", "rb") as f:
        data = f.read()
    st.download_button("DOWNLOAD REPORT", data, file_name="Annual Report.pdf")

