#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug 17 16:44:08 2017

@author: Arpit
"""

from selenium import webdriver
from database_connection import establish_connection
import os
import pandas as pd
from shutil import copy2

import schedule
import time

chromeOptions = webdriver.ChromeOptions()
directory = os.getcwd()
prefs = {"download.default_directory" : directory}
chromeOptions.add_experimental_option("prefs",prefs)
    
#script that has to run every 8th day to download the weather data and update local database
def job():
    """Method Description: Script to open a webpage, select checkboxes and click on submit button.
    """

    #instantiate google chrome driver with predefined options
    driver = webdriver.Chrome(chrome_options=chromeOptions)
   
    #link of the website to download the weather data
    driver.get("https://www.meteoblue.com/en/weather/archive/export/munich_germany_2867714")
    
    #Selecting the checkboxes
    relative_humidity_checkbox = driver.find_elements_by_xpath("//*[@id=\"wrapper-main\"]/div[1]/main/div/div[3]/form/div[4]/div[1]/div[2]/label");
    relative_humidity_checkbox[0].click();
    
    pressure_checkbox = driver.find_elements_by_xpath("//*[@id=\"wrapper-main\"]/div[1]/main/div/div[3]/form/div[4]/div[1]/div[3]/label");
    pressure_checkbox[0].click();
    
    solar_radiation_checkbox = driver.find_elements_by_xpath("//*[@id=\"wrapper-main\"]/div[1]/main/div/div[3]/form/div[4]/div[1]/div[8]/label")
    solar_radiation_checkbox[0].click();
    
    snowfall_checkbox = driver.find_elements_by_xpath("//*[@id=\"wrapper-main\"]/div[1]/main/div/div[3]/form/div[4]/div[1]/div[5]/label");
    snowfall_checkbox[0].click();
    
    cloud_cover_checkbox = driver.find_elements_by_xpath("//*[@id=\"71;sfc\"]");
    cloud_cover_checkbox[0].click();
    
    total_cloud_cover_checkbox = driver.find_elements_by_xpath("//*[@id=\"wrapper-main\"]/div[1]/main/div/div[3]/form/div[4]/div[1]/div[7]/label");
    total_cloud_cover_checkbox[0].click();
    
    #clicking the submit button to download the file locally
    elem = driver.find_elements_by_xpath("//*[@id=\"wrapper-main\"]/div[1]/main/div/div[3]/form/div[4]/div[2]/input")
    elem[0].click();

    
    #getting the handle of newly downloaded csv file
    while True:
        flag = False;
        for file in os.listdir(directory):
            if file.endswith(".csv"):
                file_to_read = os.path.join(directory, file)
                flag = True;
    #waiting if the file is downloaded before closing the driver           
        if(flag==False):
            time.sleep(10);
        else:
            break;
    
    driver.close(); #file is downloaded and connection is closed

    #Parsing the weather data csv
    df = pd.read_csv(file_to_read,sep=';', skiprows=list(range(11)))

    #archiving the file for future use
    download_directory = directory + "/downloads/";
    copy2(file_to_read, download_directory)
    
    #removing the file from the source directory
    os.remove(file_to_read);
    
    #estabilishing connection to ODTS mongodb server
    db= establish_connection("mongodb://192.168.21.240","fortiss");
    #inserting the weather data into mongodb collection
    db.collection.insert_many(df.to_dict('records'))

schedule.every(8).days.do(job)

#starting the timer
while True:
    schedule.run_pending()
    time.sleep(1)





