#!/usr/bin/env python
# coding: utf-8

# In[24]:


import sys
from stackapi import StackAPI, StackAPIError
from iso3166 import countries
SITENAME = 'stackoverflow'
SITE = StackAPI(SITENAME)
SITE = StackAPI(SITENAME,key = 'bkpnLeaeXnpiAKADkoo2ig((')
SITE.page_size = 100
import json
import unidecode
import datetime 
import time
import pandas as pd
import psycopg2
import numpy as np
import psycopg2.extras as extras
from html import unescape
from airflow.providers.postgres.hooks.postgres import PostgresHook
pd.set_option('display.max_columns', None)


# In[17]:


def return_chunks(lst,n):
    return [lst[i:i + n] for i in range(0, len(lst), n)]
def create_country_column(df):
    df.location=df.location.fillna('Unknown')
    list_ = []
    for location in df.location:
        match = next((x for x in countries_list if x in unescape(location)), 'Unknown')
        list_.append(match)
    df['country'] = list_
    df.country.map({'USA': 'United States of America', 'UK': 'United Kingdom','México':'Mexico','España':'Spain'})


# In[5]:


countries_list = ['Afghanistan', 'Aland Islands', 'Albania', 'Algeria', 'American Samoa', 'Andorra', 'Angola', 'Anguilla', 'Antarctica', 'Antigua and Barbuda', 'Argentina', 'Armenia', 'Aruba', 'Australia', 'Austria', 'Azerbaijan', 'Bahamas', 'Bahrain', 'Bangladesh', 'Barbados', 'Belarus', 'Belgium', 'Belize', 'Benin', 'Bermuda', 'Bhutan', 'Bolivia, Plurinational State of', 'Bonaire, Sint Eustatius and Saba', 'Bosnia and Herzegovina', 'Botswana', 'Bouvet Island', 'Brazil', 'British Indian Ocean Territory', 'Brunei Darussalam', 'Bulgaria', 'Burkina Faso', 'Burundi', 'Cambodia', 'Cameroon', 'Canada', 'Cape Verde', 'Cayman Islands', 'Central African Republic', 'Chad', 'Chile', 'China', 'Christmas Island', 'Cocos (Keeling) Islands', 'Colombia', 'Comoros', 'Congo', 'Congo, The Democratic Republic of the', 'Cook Islands', 'Costa Rica', "Côte d'Ivoire", 'Croatia', 'Cuba', 'Curaçao', 'Cyprus', 'Czech Republic', 'Denmark', 'Djibouti', 'Dominica', 'Dominican Republic', 'Ecuador', 'Egypt', 'El Salvador', 'Equatorial Guinea', 'Eritrea', 'Estonia', 'Ethiopia', 'Falkland Islands (Malvinas)', 'Faroe Islands', 'Fiji', 'Finland', 'France', 'French Guiana', 'French Polynesia', 'French Southern Territories', 'Gabon', 'Gambia', 'Georgia', 'Germany', 'Ghana', 'Gibraltar', 'Greece', 'Greenland', 'Grenada', 'Guadeloupe', 'Guam', 'Guatemala', 'Guernsey', 'Guinea', 'Guinea-Bissau', 'Guyana', 'Haiti', 'Heard Island and McDonald Islands', 'Holy See (Vatican City State)', 'Honduras', 'Hong Kong', 'Hungary', 'Iceland', 'India', 'Indonesia', 'Iran, Islamic Republic of', 'Iraq', 'Ireland', 'Isle of Man', 'Israel', 'Italy', 'Jamaica', 'Japan', 'Jersey', 'Jordan', 'Kazakhstan', 'Kenya', 'Kiribati', "Korea, Democratic People's Republic of", 'Korea, Republic of', 'Kuwait', 'Kyrgyzstan', "Lao People's Democratic Republic", 'Latvia', 'Lebanon', 'Lesotho', 'Liberia', 'Libya', 'Liechtenstein', 'Lithuania', 'Luxembourg', 'Macao', 'Macedonia, Republic of', 'Madagascar', 'Malawi', 'Malaysia', 'Maldives', 'Mali', 'Malta', 'Marshall Islands', 'Martinique', 'Mauritania', 'Mauritius', 'Mayotte', 'Mexico', 'Micronesia, Federated States of', 'Moldova, Republic of', 'Monaco', 'Mongolia', 'Montenegro', 'Montserrat', 'Morocco', 'Mozambique', 'Myanmar', 'Namibia', 'Nauru', 'Nepal', 'Netherlands', 'New Caledonia', 'New Zealand', 'Nicaragua', 'Niger', 'Nigeria', 'Niue', 'Norfolk Island', 'Northern Mariana Islands', 'Norway', 'Oman', 'Pakistan', 'Palau', 'Palestinian Territory, Occupied', 'Panama', 'Papua New Guinea', 'Paraguay', 'Peru', 'Philippines', 'Pitcairn', 'Poland', 'Portugal', 'Puerto Rico', 'Qatar', 'Réunion', 'Romania', 'Russian Federation', 'Rwanda', 'Saint Barthélemy', 'Saint Helena, Ascension and Tristan da Cunha', 'Saint Kitts and Nevis', 'Saint Lucia', 'Saint Martin (French part)', 'Saint Pierre and Miquelon', 'Saint Vincent and the Grenadines', 'Samoa', 'San Marino', 'Sao Tome and Principe', 'Saudi Arabia', 'Senegal', 'Serbia', 'Seychelles', 'Sierra Leone', 'Singapore', 'Sint Maarten (Dutch part)', 'Slovakia', 'Slovenia', 'Solomon Islands', 'Somalia', 'South Africa', 'South Georgia and the South Sandwich Islands', 'Spain', 'Sri Lanka', 'Sudan', 'Suriname', 'South Sudan', 'Svalbard and Jan Mayen', 'Swaziland', 'Sweden', 'Switzerland', 'Syrian Arab Republic', 'Taiwan, Province of China', 'Tajikistan', 'Tanzania, United Republic of', 'Thailand', 'Timor-Leste', 'Togo', 'Tokelau', 'Tonga', 'Trinidad and Tobago', 'Tunisia', 'Turkey', 'Turkmenistan', 'Turks and Caicos Islands', 'Tuvalu', 'Uganda', 'Ukraine', 'United Arab Emirates', 'United Kingdom', 'United States', 'United States Minor Outlying Islands', 'Uruguay', 'Uzbekistan', 'Vanuatu', 'Venezuela, Bolivarian Republic of', 'Viet Nam', 'Virgin Islands, British', 'Virgin Islands, U.S.', 'Wallis and Futuna', 'Yemen', 'Zambia', 'Zimbabwe','UK','USA','México','España','Iran']




def insert_df_into_table(df,table):  
    tuples = [tuple(x) for x in df.to_numpy()]
  
    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    #conn = psycopg2.connect( database="stackoverflow", user='postgres', password='postgres', host='127.0.0.1', port= '5432',options=f'-c search_path=raw')
    postgres_hook = PostgresHook(postgres_conn_id="LOCAL")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
    print(table," the dataframe is inserted")
    cursor.close()


# In[76]:



def stackoverflow_api_to_postgres_local(ds=None, **kwargs):
    print(ds)
    #create_raw_tables()
    datetime_object = datetime.datetime.strptime(ds, '%Y-%m-%d')
    fromtime = time.mktime(datetime_object.timetuple())
    totime = datetime_object + datetime.timedelta(days=1)
    totime = time.mktime(totime.timetuple())
    questions = SITE.fetch('questions',tagged=['pandas','numpy'],fromdate=int(fromtime),todate=int(totime) ,sort='votes')
    time.sleep(5)
    if questions['quota_remaining'] < 10:
        print('quota limit exceeded')
        sys.exit()
    else:
        print('quotra remaining:'+str(questions['quota_remaining']))
        df = pd.json_normalize(questions['items'])
        df.rename(columns=lambda x: x.replace('.','_'), inplace=True)
        df.replace({pd.NaT: 'None'}, inplace=True)
        df = df[df.columns.drop(list(df.filter(regex='migrated_from')))]
        df['tags'] = df['tags'].map(lambda x: str(x)[1:-1])
        df['tags'] = df['tags'].map(lambda x :",".join(sorted(x.split(','))) )
        df['tags'] = df['tags'].map(lambda x :x.replace("'","").strip())
        df['insert_date'] = pd.to_datetime(fromtime, unit='s')
        insert_df_into_table(df,'raw.questions')
        
        question_ids_list = df['question_id'].tolist()
        questions_chunk = return_chunks(question_ids_list,100)
        for question in questions_chunk:
            answers = SITE.fetch('questions/{ids}/answers', ids=question)
            df_answers = pd.json_normalize(answers['items'])
            df_answers.rename(columns=lambda x: x.replace('.','_'), inplace=True)
            df_answers['insert_date'] = pd.to_datetime(fromtime, unit='s')
            insert_df_into_table(df_answers,'raw.answers')

        time.sleep(5)

        df.owner_user_id = df.owner_user_id.fillna(-1)
        df_answers.owner_user_id = df_answers.owner_user_id.fillna(-1)
    
        df.owner_user_id = df.owner_user_id.astype('int',errors='ignore')
        df_answers.owner_user_id = df_answers.owner_user_id.astype('int',errors='ignore')
    
        users_ids_list = df['owner_user_id'].tolist()
        answers_users_ids_list = df_answers['owner_user_id'].tolist()
    
        merged_list=list(set(users_ids_list + answers_users_ids_list))
        user_ids_chunk = return_chunks(merged_list,100)
        for user in user_ids_chunk:
            users = SITE.fetch('users/{ids}', ids=user)
            df_users = pd.json_normalize(users['items'])
            df_users.rename(columns=lambda x: x.replace('.','_'), inplace=True)
            create_country_column(df_users)
            df_users['insert_date'] = pd.to_datetime(fromtime, unit='s')
            insert_df_into_table(df_users,'raw.users')
        time.sleep(5)


# In[ ]:





