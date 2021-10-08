# -*- coding: utf-8 -*-
"""
Created on Fri Jul 23 20:27:54 2021

@author: INFA
"""
import datetime
import pymysql.cursors
import time
import requests
import dateutil
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import influxdb
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)



while True:
    try:    
        b=datetime.datetime.now()
        print(b)
        date_from=(b + datetime.timedelta(minutes=-3)).replace(second=0, microsecond=0).strftime('%Y-%m-%d-%H:%M:%S')
        date_to=(b + datetime.timedelta(minutes=-2)).replace(second=0, microsecond=0).strftime('%Y-%m-%d-%H:%M:%S')
        connection = pymysql.connect(host='xxx',
                                     user='xxx',
                                     password='xxx',
                                     database='xxx',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        cursor=connection.cursor()
        #sql = "select * from tbl_gob_distortedglass where Line=31 ORDER BY datetimeid DESC LIMIT 64"
        sql = "select * from tbl_gob_distortedglass where Line=31 and datetimeid >='"+date_from+"' and datetimeid <'"+date_to+"'"
        cursor.execute(sql)#, ('webmaster@python.org',))
        tupples = cursor.fetchall()
        client = influxdb.InfluxDBClient('xxx',xxx, "xxx", "xxx")        
        fields_dict={}
        #json_array=[]
        #for i in range(32,64):
        print(len(tupples))
        for i in range(len(tupples)):
            
            date_tz = tupples[i]['DateTimeID'].replace(tzinfo=dateutil.tz.tzlocal())
            for n in range(32):
                fields_dict['Z'+str(n)+'_norm']=tupples[i]['Zone'+str(n)+'_avg']/tupples[i]['TotalIntensity_avg']
            
            fields_dict['NumberOfMeasurements']=tupples[i]['NumberOfMeasurements']
            json_body = [
                    {
                            'measurement': 'Xpar32DG',
                            'time': date_tz,
                            'tags':{"CAV": tupples[i]['Cavity'].replace('B','D')},
                            'fields':fields_dict
                            }
                    ]
            #json_array.append(json_body)
            client.write_points(json_body, database='xxx')        
        #client.write_points(json_body, database='xxx')
        print(json_body[0]['fields']['NumberOfMeasurements'],json_body[0]['time'])
        client.close()
        connection.close()
        #time.sleep(60)
        b=datetime.datetime.now()   
        time.sleep(20+((b.replace(second=0, microsecond=0)+datetime.timedelta(minutes=1))-b).seconds)
    except :
        print("Try failed!")
        #connection.close()
        client.close()
        time.sleep(30)
    


