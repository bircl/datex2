# -*- coding: utf-8 -*-
"""
Spyder Editor

point: a coordinate pair
Linear: 2 points
Itineary: Route
ItineraryByIndexedLocations
linear
"""
import sys
sys.path.insert(0, r'C:\Users\BirkanChalashkan\OneDrive - Wessex Internet\Documents\one_network/')

# Import the necessary libraries
import pandas as pd # for working with dataframes
import geopandas as gpd # for working with spatial data
from shapely.geometry import Point, LineString # for creating geometry objects
import numpy as np # for working with numeric data

import lxml.etree as et # for parsing XML files
from api_request import download_on_data_feed # for downloading data from an API
from time import sleep # for adding delays in the script
from datetime import datetime # for working with date and time data

while 1:
    

    df_columns=['situation_id', 'publisher', 'datelastmodified', 'works_category', 'works_state','responsbile_org', 'start_time', 'end_time', 'permit_status', 'comment', 'geometry']
    
    
    competitors_list = ['gigaclear' , 'jurassic', 'openreach', 'voneus', 'truespeed', 'cityfibre', 
                        'airband', 'trooli', 'giganet' , 'cityfibre', 'virgin media']
    
    competitors_full_name_list = ['Gigaclear Limited', 'Jurassic Fibre Limited', 'Openreach', 
                                  'Voneus Ltd', 'Truespeed Communications Ltd', 'CITYFIBRE METRO NETWORKS LTD', 
                                  'Airband Community Internet Ltd', 'Trooli', 'Giganet', 'CITYFIBRE METRO NETWORKS LTD',
                                  'Virgin Media'
                                  ]
    
    df = pd.DataFrame(columns=df_columns)
    
    def find_element(element,element_name, text, namespace):
        element.find('.//'+namespace+element_name)
        element.find('.//'+namespace+element_name).text
    
    xml_path = r'C:\Users\BirkanChalashkan\OneDrive - Wessex Internet\Documents\one_network\data/'
    sharepoint_path = r'C:\Users\BirkanChalashkan\Wessex Internet\Strategy - Documents\02_Strategy Data Projects\01_One.network\data\Spatial/'
    main_gdf = gpd.read_file(xml_path + '/spatial/One Network Main Dataframe.gpkg')

    
    file_path, date_time = download_on_data_feed()
    
    # LOAD XML AND XSL FILES
    root = et.parse(file_path).getroot()
    
    ns = {
        'd':'http://datex2.eu/schema/2/2_0'
    }
    
    namespace = '{http://datex2.eu/schema/2/2_0}'
    
    situation = root.xpath('//d:situationRecord', namespaces=ns)
    publisher = root.xpath('//d:sourceName', namespaces=ns)
    
    def get_address(element):
        address = element.findall('.//'+namespace+'name') #get address information
        #children_list = ['linkName', 'areaName', 'townName', 'administrativeAreaName', 'countyName']
        #var_list = ['linkName', 'areaName', 'townName', 'administrativeAreaName', 'countyName']
        street,locality,town,administrative,county = None,None,None,None,None
        address_list = []
        for a in address:
            if a[1].text == 'linkName':
                street = a[0][0][0].text #get street
                address_list.append(street)
            if a[1].text == 'areaName':
                locality = a[0][0][0].text #get street
                address_list.append(locality)
            if a[1].text == 'townName':
                town = a[0][0][0].text #get street
                address_list.append(town)
            if a[1].text == 'administrativeAreaName':
                administrative = a[0][0][0].text #get street
                address_list.append(administrative)
            if a[1].text == 'countyName':
                county = a[0][0][0].text #get street
        roadlocation_street = ','.join(address_list)
        return roadlocation_street, street,locality,town,administrative,county
    
    for j in situation: #iterate situations
        temp_list = [] #temporary list for elements
        try:
            responsible_org = None
            responsible_org = j.find('.//'+namespace+'responsibleOrganisationName').text #read responsible organization
            situation_location = j.find('.//'+namespace+'groupOfLocations')
            location_type = situation_location.attrib['{http://www.w3.org/2001/XMLSchema-instance}type']
            len_location = len(situation_location)
            
            roadlocation_street,street_descriptor,locality,town_name,administrative_name,county_name = get_address(situation_location)
            situation_id = j.find('.//'+namespace+'situationRecordCreationReference').text
            publisher = j.find('.//'+namespace+'sourceName')[0][0].text
            datelastmodified = j.find('.//'+namespace+'situationRecordVersionTime').text
            works_category = j.find('.//'+namespace+'worksCategory')[0].text
            works_state = j.find('.//'+namespace+'worksState')[1].text
            permit_status = j.find('.//'+namespace+'permitStatus')[1].text
            start_time = j.find('.//'+namespace+'overallStartTime').text
            end_time = j.find('.//'+namespace+'overallEndTime').text
        except:
            pass
        comment = j.find('.//'+namespace+'comment')[0] #define comment element
        comment_list = [] #initiate a list for comments
        for c in comment:
            subcomment = c.text #get the text of subcomments
            comment_list.append(subcomment) #append subcomments to the list
        comments = " - ".join(comment_list) #join comments together
        try:
            if len_location == 1:
                situation_coordinates = situation_location.findall('.//'+namespace+'pointCoordinates')
                lat = np.float64(situation_coordinates[0][0].text)
                long = np.float64(situation_coordinates[0][1].text)
                geometry = Point(long, lat)
            else:
                line_list = []
                situation_coordinates = situation_location.findall('.//'+namespace+'pointCoordinates')
                for l in situation_coordinates:
                    lat = np.float64(l[0].text)
                    long = np.float64(l[1].text)
                    p = Point(long, lat)
                    line_list.append(p)
                geometry  = LineString(line_list)
        except:
            pass
        temp_list = [situation_id, publisher, datelastmodified, works_category, works_state, responsible_org, start_time, end_time,permit_status, comments, geometry]
        temp_df = pd.DataFrame([temp_list], columns=df_columns)
        df = pd.concat([df, temp_df])
    
    def filter_df(df):
        #fix missing resp organisation
        for cn,cf in zip(competitors_list, competitors_full_name_list):
            df.loc[(df['comment'].str.contains(cn, na=False, case = False) & df['responsbile_org'].isnull()), ['responsbile_org']] = cf
        #filter the data within our competitors
        df[df['responsbile_org'].str.contains('|'.join(competitors_list), case=False, regex=True)]
        df[df['comment'].str.contains('fibre', case=False)]
        
    gdf = gpd.GeoDataFrame(df)
    new_main_gdf = pd.concat([main_gdf, gdf])
    
    new_main_gdf = new_main_gdf[(~new_main_gdf.duplicated(subset = ['situation_id', 'geometry'])) | (new_main_gdf['situation_id'].isnull())]
    
    #new_main_gdf.to_file(r'C:\Users\BirkanChalashkan\OneDrive - Wessex Internet\Desktop/' + 'main_gdf.gpkg')
    
    new_main_gdf.to_file(sharepoint_path + 'main_gdf.gpkg')
    new_main_gdf.to_file(xml_path + '/spatial/One Network Main Dataframe.gpkg')
    new_main_gdf.to_csv(xml_path + '/csv/main_gdf.csv')
    
    gdf.to_file(xml_path + f'spatial\daily_api_request/one_network_{date_time}.gpkg')
    gdf.to_csv(xml_path + f'csv\daily_api_request/one_network_{date_time}.csv')
    time_now = datetime.now().strftime("%H:%M:%S - %d/%B")
    print('Main GDF updated ' + time_now)
    sleep(3000)
