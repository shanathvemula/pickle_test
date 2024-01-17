import pandas as pd
import psycopg2
from pymongo import MongoClient


def PostgreSQL(dbname, username, password, port, schema, table, host, save=False, report=False):
    try:
        # print(credentials)
        credentials = {"dbname": dbname, "user": username, "password": password, "host": host, "port": port,
                       "schema": schema, "table": table}
        schema = credentials.pop('schema', None)
        table = credentials.pop('table', None)
        conn = psycopg2.connect(**credentials)
        sql = f"SELECT * FROM {schema + '.' + table}"
        # print(sql)
        credentials['schema'] = schema
        credentials['table'] = table
        data = pd.read_sql(sql, conn)
        # print(data)
        conn.close()
        file_url = "None"
        file_path = "None"
        # if report:
        #     file_path, file_url = generate_report(data, credentials['table'])
        # print(file_path, file_url)
        # if save:
        #     credentials['file_url'] = "None"
        #     credentials['report_url'] = file_url
        #     load_and_save(credentials=credentials, datasource="PostgreSQL")
        return data, file_url, file_path
    except Exception as e:
        # print(str(e))
        return str(e)


# print(PostgreSQL(dbname='tech', username='tdm_admin', password="tdmpfx@123", port=5432, schema="public",
#                  host='192.168.15.54', table="task"))

def MongoDB(dbname, collection, host, port, schema='mongodb', user='', password='', uri=None, save=False,
            report=False):  # credentials,
    try:
        # print(credentials)
        if not uri:
            if user == "" or password == "":
                uri = schema + "://" + host + ":" + str(port)
            else:
                uri = schema + "://" + user + ":" + password + "@" + host + ":" + str(port)
        # if "uri" in credentials.keys():
        #     uri = credentials['uri']
        # elif credentials["user"] == "" or credentials["password"] == "":
        #     uri = credentials['schema'] + "://" + credentials['host'] + ":" + \
        #           credentials['port']
        # else:
        #     uri = credentials['schema'] + "://" + credentials['user'] + ":" + \
        #           credentials['password'] + "@" + credentials['host'] + ":" + \
        #           credentials['port']
        # print(uri)
        client = MongoClient(uri)
        db = client[dbname]  # client[credentials['dbname']]
        collection = db[collection]  # db[credentials['collection']]
        data = pd.DataFrame(list(collection.find()))
        # print(data)
        if '_id' in data.columns:
            data._id = data._id.astype('str')
        # print(data)
        # if save:
        #     credentials['table'] = credentials['collection']
        #     credentials['file_url'] = "None"
        #     load_and_save(credentials=credentials, datasource="MongoDB")
        file_url = "None"
        file_path = "None"
        # if report:
        #     file_path, file_url = generate_report(data, credentials['collection'])
        # print(file_path, file_url)
        # if save:
        #     credentials['table'] = credentials['collection']
        #     credentials['file_url'] = "None"
        #     credentials['report_url'] = file_url
        #     load_and_save(credentials=credentials, datasource="MongoDB")
        return data, file_url, file_path
        # print(data)
        # return data
    except Exception as e:
        print(e)
        return str(e)


# print(MongoDB(dbname='config', collection='thadam_config', host='192.168.15.54', port=27017))


