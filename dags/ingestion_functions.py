from stackapi import StackAPI
from iso3166 import countries
SITENAME = 'stackoverflow'
SITE = StackAPI(SITENAME,key = 'bkpnLeaeXnpiAKADkoo2ig((')
SITE.page_size = 100
import json
import unidecode
import datetime 
import time
import pandas as pd
import psycopg2
import numpy as np
import sys
import psycopg2.extras as extras
from html import unescape
import csv
pd.set_option('display.max_columns', None)
from airflow.providers.postgres.hooks.postgres import PostgresHook

countries_list = ['Afghanistan', 'Aland Islands', 'Albania', 'Algeria', 'American Samoa', 'Andorra', 'Angola', 'Anguilla', 'Antarctica', 'Antigua and Barbuda', 'Argentina', 'Armenia', 'Aruba', 'Australia', 'Austria', 'Azerbaijan', 'Bahamas', 'Bahrain', 'Bangladesh', 'Barbados', 'Belarus', 'Belgium', 'Belize', 'Benin', 'Bermuda', 'Bhutan', 'Bolivia, Plurinational State of', 'Bonaire, Sint Eustatius and Saba', 'Bosnia and Herzegovina', 'Botswana', 'Bouvet Island', 'Brazil', 'British Indian Ocean Territory', 'Brunei Darussalam', 'Bulgaria', 'Burkina Faso', 'Burundi', 'Cambodia', 'Cameroon', 'Canada', 'Cape Verde', 'Cayman Islands', 'Central African Republic', 'Chad', 'Chile', 'China', 'Christmas Island', 'Cocos (Keeling) Islands', 'Colombia', 'Comoros', 'Congo', 'Congo, The Democratic Republic of the', 'Cook Islands', 'Costa Rica', "Côte d'Ivoire", 'Croatia', 'Cuba', 'Curaçao', 'Cyprus', 'Czech Republic', 'Denmark', 'Djibouti', 'Dominica', 'Dominican Republic', 'Ecuador', 'Egypt', 'El Salvador', 'Equatorial Guinea', 'Eritrea', 'Estonia', 'Ethiopia', 'Falkland Islands (Malvinas)', 'Faroe Islands', 'Fiji', 'Finland', 'France', 'French Guiana', 'French Polynesia', 'French Southern Territories', 'Gabon', 'Gambia', 'Georgia', 'Germany', 'Ghana', 'Gibraltar', 'Greece', 'Greenland', 'Grenada', 'Guadeloupe', 'Guam', 'Guatemala', 'Guernsey', 'Guinea', 'Guinea-Bissau', 'Guyana', 'Haiti', 'Heard Island and McDonald Islands', 'Holy See (Vatican City State)', 'Honduras', 'Hong Kong', 'Hungary', 'Iceland', 'India', 'Indonesia', 'Iran, Islamic Republic of', 'Iraq', 'Ireland', 'Isle of Man', 'Israel', 'Italy', 'Jamaica', 'Japan', 'Jersey', 'Jordan', 'Kazakhstan', 'Kenya', 'Kiribati', "Korea, Democratic People's Republic of", 'Korea, Republic of', 'Kuwait', 'Kyrgyzstan', "Lao People's Democratic Republic", 'Latvia', 'Lebanon', 'Lesotho', 'Liberia', 'Libya', 'Liechtenstein', 'Lithuania', 'Luxembourg', 'Macao', 'Macedonia, Republic of', 'Madagascar', 'Malawi', 'Malaysia', 'Maldives', 'Mali', 'Malta', 'Marshall Islands', 'Martinique', 'Mauritania', 'Mauritius', 'Mayotte', 'Mexico', 'Micronesia, Federated States of', 'Moldova, Republic of', 'Monaco', 'Mongolia', 'Montenegro', 'Montserrat', 'Morocco', 'Mozambique', 'Myanmar', 'Namibia', 'Nauru', 'Nepal', 'Netherlands', 'New Caledonia', 'New Zealand', 'Nicaragua', 'Niger', 'Nigeria', 'Niue', 'Norfolk Island', 'Northern Mariana Islands', 'Norway', 'Oman', 'Pakistan', 'Palau', 'Palestinian Territory, Occupied', 'Panama', 'Papua New Guinea', 'Paraguay', 'Peru', 'Philippines', 'Pitcairn', 'Poland', 'Portugal', 'Puerto Rico', 'Qatar', 'Réunion', 'Romania', 'Russian Federation', 'Rwanda', 'Saint Barthélemy', 'Saint Helena, Ascension and Tristan da Cunha', 'Saint Kitts and Nevis', 'Saint Lucia', 'Saint Martin (French part)', 'Saint Pierre and Miquelon', 'Saint Vincent and the Grenadines', 'Samoa', 'San Marino', 'Sao Tome and Principe', 'Saudi Arabia', 'Senegal', 'Serbia', 'Seychelles', 'Sierra Leone', 'Singapore', 'Sint Maarten (Dutch part)', 'Slovakia', 'Slovenia', 'Solomon Islands', 'Somalia', 'South Africa', 'South Georgia and the South Sandwich Islands', 'Spain', 'Sri Lanka', 'Sudan', 'Suriname', 'South Sudan', 'Svalbard and Jan Mayen', 'Swaziland', 'Sweden', 'Switzerland', 'Syrian Arab Republic', 'Taiwan, Province of China', 'Tajikistan', 'Tanzania, United Republic of', 'Thailand', 'Timor-Leste', 'Togo', 'Tokelau', 'Tonga', 'Trinidad and Tobago', 'Tunisia', 'Turkey', 'Turkmenistan', 'Turks and Caicos Islands', 'Tuvalu', 'Uganda', 'Ukraine', 'United Arab Emirates', 'United Kingdom', 'United States', 'United States Minor Outlying Islands', 'Uruguay', 'Uzbekistan', 'Vanuatu', 'Venezuela, Bolivarian Republic of', 'Viet Nam', 'Virgin Islands, British', 'Virgin Islands, U.S.', 'Wallis and Futuna', 'Yemen', 'Zambia', 'Zimbabwe','UK','USA','México','España','Iran']

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

def insert_csv_into_postgres_table(date,table):  
    #tuples = [tuple(x) for x in df.to_numpy()]
    
    with open("/opt/airflow/dags/files/raw/"+table+"/"+table+"-"+str(date)+".csv", 'r',encoding='utf-8') as f:
    #with open("raw/"+table+"/"+table+"-"+str(date)+".csv", 'r',encoding='utf-8') as f:
        data=[tuple(line) for line in csv.reader(f)]
    tuples = data[1:]
    cols = ','.join(list(data[0]))
    #print(cols)    

    #cols = ','.join(list(df.columns))
    # SQL query to execute
    delete_query = "TRUNCATE TABLE raw.\"%s\" " % (table)
    print(delete_query)
    query = "INSERT INTO raw.\"%s\"(%s) VALUES %%s" % (table, cols)
    print(query)
    postgres_hook = PostgresHook(postgres_conn_id="LOCAL")
    conn = postgres_hook.get_conn()
    #conn = psycopg2.connect( database="stackoverflow", user='postgres', password='postgres', host='127.0.0.1', port= '5432',options=f'-c search_path=raw')
    cursor = conn.cursor()
    try:
        cursor.execute(delete_query)
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
    print(table,"inserted")
    cursor.close()
    
def stackoverflow_api_to_csv(ds):
    SITENAME = 'stackoverflow'
    SITE = StackAPI(SITENAME,key = 'bkpnLeaeXnpiAKADkoo2ig((')
    SITE.page_size = 100
    #create_raw_tables()
    date = ds
    datetime_object = datetime.datetime.strptime(date, '%Y-%m-%d')
    fromtime = time.mktime(datetime_object.timetuple())
    totime = datetime_object + datetime.timedelta(days=1)
    totime = time.mktime(totime.timetuple())

    with open('/opt/airflow/dags/files/raw/quota.log', 'r') as fileToRead:
        value = fileToRead.read()
        print(value)
        if(int(value) < 100):
            print('quota exceeded')
            sys.exit()

    questions = SITE.fetch('questions',tagged=['pandas','numpy'],fromdate=int(fromtime),todate=int(totime) ,sort='votes')
    with open('/opt/airflow/dags/files/raw/quota.log', 'w') as filetowrite:
        filetowrite.write(str(questions['quota_remaining']))
    time.sleep(1)
    
    if questions['backoff'] > 0:
        time.sleep(int(questions['backoff']))
    else:
        print('quotra remaining:'+str(questions['quota_remaining']))
        df = pd.json_normalize(questions['items'])
        df.rename(columns=lambda x: x.replace('.','_'), inplace=True)
        df.replace({pd.NaT: 'None'}, inplace=True)
        df = df[df.columns.drop(list(df.filter(regex='migrated_from')))]
        df['tags'] = df['tags'].map(lambda x: str(x)[1:-1])
        df['tags'] = df['tags'].map(lambda x :x.replace(", ",",").strip())
        df['tags'] = df['tags'].map(lambda x :",".join(sorted(x.split(','))) )
        df['tags'] = df['tags'].map(lambda x :x.replace("'","").strip())
        df['insert_date'] = pd.to_datetime(date,format='%Y-%m-%d' )
        
        file_name= "/opt/airflow/dags/files/raw/question/question-"+str(date)+".csv"
        df.to_csv(file_name,index=False)
        
        
        answers_resultant_df = pd.DataFrame()
        question_ids_list = df['question_id'].tolist()
        questions_chunk = return_chunks(question_ids_list,100)
        for question in questions_chunk:
            time.sleep(1)
            answers = SITE.fetch('questions/{ids}/answers', ids=question)
            with open('/opt/airflow/dags/files/raw/quota.log', 'w') as filetowrite:
                filetowrite.write(str(answers['quota_remaining']))
            
            if answers['backoff'] > 0:
                time.sleep(int(answers['backoff']))
                
            else:
                df_answers = pd.json_normalize(answers['items'])
                df_answers.rename(columns=lambda x: x.replace('.','_'), inplace=True)
                df_answers['insert_date'] = pd.to_datetime(date,format='%Y-%m-%d' )
                answers_resultant_df=answers_resultant_df.append(df_answers, ignore_index=True)
        
        file_name = "/opt/airflow/dags/files/raw/answer/answer-"+str(date)+".csv"
        answers_resultant_df.to_csv(file_name,index=False)
        
        df.owner_user_id = df.owner_user_id.fillna(-1)
        df_answers.owner_user_id = df_answers.owner_user_id.fillna(-1)
    
        df.owner_user_id = df.owner_user_id.astype('int',errors='ignore')
        df_answers.owner_user_id = df_answers.owner_user_id.astype('int',errors='ignore')
    
        users_ids_list = list(set(df['owner_user_id']))
        answers_users_ids_list = list(set(df_answers['owner_user_id']))
    
        merged_list=list(set(users_ids_list + answers_users_ids_list))

        user_ids_chunk = return_chunks(merged_list,100)
        users_resultant_df = pd.DataFrame()
        for user in user_ids_chunk:
            time.sleep(1)
            users = SITE.fetch('users/{ids}', ids=user)
            with open('/opt/airflow/dags/files/raw/quota.log', 'w') as filetowrite:
                filetowrite.write(str(users['quota_remaining']))
            if users['backoff'] > 0:
                time.sleep(int(answers['backoff']))
            else:
                df_users = pd.json_normalize(users['items'])
                df_users.rename(columns=lambda x: x.replace('.','_'), inplace=True)
                create_country_column(df_users)
                df_users['insert_date'] = pd.to_datetime(date,format='%Y-%m-%d' )
                users_resultant_df=users_resultant_df.append(df_users, ignore_index=True)
        
        
        file_name = "/opt/airflow/dags/files/raw/account/account-"+str(date)+".csv"
        users_resultant_df.to_csv(file_name,index=False)

def insert_all_csv_files_into_postgres_table(ds):
    insert_csv_into_postgres_table(ds,'question')
    insert_csv_into_postgres_table(ds,'answer')
    insert_csv_into_postgres_table(ds,'account')