import psycopg2
import re
import pandas as pd
import json
import numpy as np
import csv
import random
import datetime
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
#
conn_string = 'postgresql://postgres:1111@localhost:3333/Veda'
conn = psycopg2.connect(conn_string)
conn.autocommit = True
cursor = conn.cursor()
#
# db = create_engine(conn_string)
# conn = db.connect()

tables=['restaurant','prices','location','paymenttypes','timings','cuisines','category','menu']

def showTable(tableName):
    sql_statement = f"select * from {tableName}"
    df= pd.read_sql_query(sql_statement, conn)
    print(df)

def getCols(tableName,curr):
    sql_statement = f"select * from {tableName}"
    curr.execute(sql_statement)
    column_names = [desc[0] for desc in curr.description]
    return column_names

def processData():
    filename="data.csv"
    header = ['id','dateAdded','dateUpdated','address','categories','primaryCategories','city','claimed','country','cuisines','descriptions.dateSeen','descriptions.sourceURLs','descriptions.value','facebookPageURL','features.key','features.value','hours.day','hours.dept','hours.hour','imageURLs','isClosed','keys','languagesSpoken','latitude','longitude','menuPageURL','menus.amountMax','menus.amountMin','menus.category','menus.currency','menus.dateSeen','menus.description','menus.name','menus.sourceURLs','name','paymentTypes','phones','postalCode','priceRangeCurrency','priceRangeMin','priceRangeMax','province','sic','sourceURLs','twitter','websites','yearOpened']
    veda_dict = []
    with open(filename) as file:
        lines=csv.reader(file,delimiter=',')
        for i,line in enumerate(lines):
            if i:
                row={}
                for j,col in enumerate(line):
                    if header[j] in ['menus.dateSeen','imageURLs']:
                        cols=col.split(',')
                        row[header[j]] = cols
                    else:
                        row[header[j]] = col
                    # if len(subHeader)>1:
                    #     if subHeader[0] in row:
                    #         headerObj=row[subHeader[0]]
                    #         headerObj[subHeader[1]]=cols
                    #     else:
                    #         row[subHeader[0]]={
                    #             subHeader[1]:cols
                    #         }
                    # else:
                row['sm_type']=['facebook','twitter']
                veda_dict.append(row)
    with open('data.json', 'w') as fp:
        json.dump(veda_dict, fp)

    return pd.DataFrame(veda_dict)

def loadData():
    veda_df = processData()

    MAX_RES_ID=random.randint(1000,9999)
    MAX_PR_ID = random.randint(1000,9999)
    MAX_TM_ID = random.randint(1000,9999)
    MAX_SM_ID = random.randint(1000,9999)
    MAX_ADD_ID = random.randint(1000,9999)
    MAX_CUS_ID = random.randint(1000,9999)
    MAX_MENU_ID = random.randint(1000,9999)
    MAX_CAT_ID = random.randint(1000,9999)
    MAX_TYP_ID = random.randint(1000,9999)
    veda_df['imageUrl'] = veda_df.apply(lambda x: x.imageURLs[0] if len(x.imageURLs)>1 else "", axis=1)
    veda_df['menu.asOfDate'] = veda_df.apply(lambda x: x['menus.dateSeen'][0] if len(x['menus.dateSeen']) > 1 else "", axis=1)
    veda_df['restaurant_id'] = range(MAX_RES_ID, MAX_RES_ID + len(veda_df))
    veda_df['id']=veda_df['restaurant_id']
    veda_df['price_id'] = range(MAX_PR_ID, MAX_PR_ID + len(veda_df))
    veda_df['address_id'] = range(MAX_ADD_ID, MAX_ADD_ID + len(veda_df))
    veda_df['timing_id'] = range(MAX_TM_ID, MAX_TM_ID + len(veda_df))

    dbColMap = {
        'claimedby': 'claimed',
        'id': 'restaurant_id',
        'description': 'descriptions.value',
        'imageurl':'imageUrl',
        'yearopened':'yearOpened',
        'isclosed':'isClosed',
        'dateadded':'dateAdded',
        'dateupdated':'dateUpdated'
    }
    menuCuiMap={}
    cuisineMap={}
    catMap = {}
    for table in tables:
        cols=getCols(table,cursor)
        if table=="restaurant":
            cols = matchDBCols(cols, dbColMap)
            inv_map = {v: k for k, v in dbColMap.items()}
            res_df=veda_df[cols]
            res_df=res_df.rename(columns=inv_map)
            loadRestaurants(table,res_df)
        elif table=="prices":
            dbPriceColMap={
                'currency': 'priceRangeCurrency',
                'price_min': 'priceRangeMin',
                'price_max': 'priceRangeMax'
            }
            cols = matchDBCols(cols, dbPriceColMap)
            inv_map = {v: k for k, v in dbPriceColMap.items()}
            pr_df = veda_df[cols]
            pr_df = pr_df.rename(columns=inv_map)
            loadPrices(table, pr_df)
        elif table == "menu":

            dbMenuColMap = {
                'name': 'menus.name',
                'description': 'menus.description',
                'category': 'menus.category',
                'pageurl': 'menuPageURL',
                'asofdate': 'menu.asOfDate'
            }
            cols = matchDBCols(cols, dbMenuColMap)
            inv_map = {v: k for k, v in dbMenuColMap.items()}
            veda_df['cuisineList']=veda_df.cuisines.str.split(',')
            veda_df['subCatList'] = veda_df.categories.str.split(',')
            veda_df = veda_df.explode('cuisineList')
            veda_df = veda_df.explode('subCatList')
            veda_df['menu_id'] = range(MAX_MENU_ID, MAX_MENU_ID + len(veda_df))
            veda_df['cuisine_id']=veda_df.apply(lambda x: cuisineMap[x['cuisineList'].strip(' ')], axis=1)
            veda_df['cat_id'] = veda_df.apply(lambda x: catMap[x['subCatList'].strip(' ')], axis=1)
            mn_df = veda_df[cols]
            mn_df = mn_df.rename(columns=inv_map)
            loadMenus(table, mn_df)
        elif table == "cuisines":
            dbCusColMap = {
                'cuisine': 'cuisines'
            }
            cuis=[]
            for i,row in veda_df.iterrows():
                cuis.append(row['cuisines'].split(','))

            cui_df=pd.DataFrame(columns=cols)
            cuisine=[i.strip(' ') for i in list(set(sum(cuis, [])))]

            inv_map = {v: k for k, v in dbCusColMap.items()}
            cui_df['cuisine_id'] = range(MAX_CUS_ID, MAX_CUS_ID + len(cuisine))
            cui_df['cuisine']=cuisine
            cui_df = cui_df.rename(columns=inv_map)
            cuisineMap=dict(zip(cui_df['cuisine'],cui_df['cuisine_id']))
            loadCuisines(table, cui_df)
        elif table == "category":
            dbCatColMap = {
                'subcategory': 'categories',
                'maincategory': 'primaryCategories'
            }
            subMainMap = []
            for i, row in veda_df.iterrows():
                subMainMap.append((row['primaryCategories'],row['categories'].split(',')))

            cats=[]
            subCats=[]
            for cat in subMainMap:
                main,v=cat
                for sub in v:
                    subCats.append(sub.strip(' '))
                    cats.append((main,sub.strip(' ')))
            # subcategory = [i.strip(' ') for i in list(set(sum(subMainMap.values(), [])))]

            cols = matchDBCols(cols, dbCatColMap)
            cats=list(set([ tuple(sorted(t)) for t in cats ]))
            cat_df=pd.DataFrame(cats, columns=['subcategory','maincategory'])
            cat_df['cat_id'] = range(MAX_CAT_ID, MAX_CAT_ID + len(cats))
            catMap = dict(zip(list(set(subCats)), cat_df['cat_id']))
            loadCategories(table, cat_df)
        elif table == "location":
            dbLocColMap = {
                'postal': 'postalCode',
                'phone': 'phones',
                'hierarchy': 'keys'
            }
            cols = matchDBCols(cols, dbLocColMap)
            inv_map = {v: k for k, v in dbLocColMap.items()}
            loc_df = veda_df[cols]
            loc_df = loc_df.rename(columns=inv_map)
            loadLocations(table, loc_df)
        elif table == "timings":
            dbTmColMap = {
                'weekday': 'hours.day',
                'time': 'hours.hour'
            }
            cols = matchDBCols(cols, dbTmColMap)
            inv_map = {v: k for k, v in dbTmColMap.items()}
            tm_df = veda_df[cols]
            tm_df = tm_df.rename(columns=inv_map)
            loadTimings(table, tm_df)
        elif table == "paymenttypes":
            dbPyColMap = {
                'type': 'paymentTypes'
            }

            cols = matchDBCols(cols, dbPyColMap)
            inv_map = {v: k for k, v in dbPyColMap.items()}
            veda_df['type_id'] = range(MAX_TYP_ID, MAX_TYP_ID + len(veda_df))
            py_df = veda_df[cols]

            py_df = py_df.rename(columns=inv_map)
            loadPayments(table, py_df)

# def getSmUrls(sm_types,veda_df):
#     sm_urls=[]
#     for type in sm_types:
#         if type=='facebook':
#             type='facebookPageURL'
#         sm_urls.append(veda_df[type])
#     return sm_urls

def loadSm(table,sm_df):
    addRecord(table, set(sm_df.itertuples(index=False, name=None)))

def loadPayments(table,py_df):
    py_df['type'] = py_df['type'].replace([''], 'VISA')
    addRecord(table, set(py_df.itertuples(index=False, name=None)))

def loadTimings(table,op_df):
    op_df['weekday'] = op_df['weekday'].replace([''],'Mon-Sat')
    op_df['time'] = op_df['time'].replace([''],'10AM-7PM')
    addRecord(table, set(op_df.itertuples(index=False, name=None)))

def loadLocations(table,op_df):
    addRecord(table, set(op_df.itertuples(index=False, name=None)))

def loadCategories(table, cat_df):
    addRecord(table, set(cat_df.itertuples(index=False, name=None)))

def loadCuisines(table, cs_df):
    addRecord(table, set(cs_df.itertuples(index=False, name=None)))

def loadMenus(table, mn_df):
    mn_df['description'] = mn_df['description'].replace([''],'Thick rice and lentil vegetable topped with mixed vegetable')
    mn_df['category'] = mn_df['category'].replace([''],'starters')
    now = datetime.datetime.utcnow()
    mn_df['asofdate'] = mn_df['asofdate'].replace([''],now.strftime('%Y-%m-%d %H:%M:%S') )
    addRecord(table, set(mn_df.itertuples(index=False, name=None)))

def loadPrices(table,pr_df):
    pr_df['currency'] = pr_df['currency'].replace([''],'USD')
    pr_df['price_max'] = pr_df['price_max'].replace([''], 14)
    pr_df['price_min'] = pr_df['price_min'].replace([''], 2)
    addRecord(table, set(pr_df.itertuples(index=False, name=None)))

def loadRestaurants(table,res_df):
    res_df['yearopened'] = res_df['yearopened'].replace([''], 1996)
    res_df['description'] = res_df['description'].replace([''], 'Vegetables sauteed and added to brown rice/noodles')
    res_df['imageurl'] = res_df['imageurl'].replace([''], 'https://www.pinterest.com/pin/93027548546302923/')
    res_df['claimedby']  = res_df['claimedby'].replace([''],'bluewebsites.com')
    res_df['sic'] = res_df['sic'].replace([''], 0)
    res_df['isclosed'] = res_df['isclosed'].replace(['TRUE', 'FALSE', ''], [True, False, False])
    addRecord(table, set(res_df.itertuples(index=False, name=None)))

def addRecord(table,data):
    cursor.execute(f"TRUNCATE TABLE {table} CASCADE",table)
    execute_values(cursor,f"""INSERT INTO {table} VALUES %s""",data)

def matchDBCols(cols,dbColMap):
    for i,col in enumerate(cols):
        if col in dbColMap:
            cols[i]=dbColMap[col]
    return cols

loadData()
# with open('data.json', 'w') as fp:
#     json.dump(veda_dict, fp)
