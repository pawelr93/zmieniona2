import pandas as pd
from sqlalchemy import create_engine
import glob
import csv
from typing import NewType 



global tableNaming
def count_number_of_rows(csvFile:str)->str:
    with open(csvFile, 'r', encoding='utf-8') as csvfile: #encoding ="utf-8", add this phrase allow avoid problem with "bajts"
        reader = csv.reader(csvfile)
        numberRows = sum(1 for row in reader) 
    return numberRows
def add_new_column(array,df,l,tableName):
    try:
      df1=df[array]
      l.append(df1)
      tableNaming[tableName]=df1
    except:
      print('An exception occurred in {tableName}')
    return tableNaming

engine = create_engine('postgresql://postgres:admin@localhost:5432/postgres')
for file in glob.glob("*.csv"):
    df = pd.read_csv(file)

    nameColumns=df.columns
    avalabilities=df['availability_30']
  
    newarray=[lambda arg=x: 30-arg for x in avalabilities]
    df.insert(1, "monthly_availability",newarray)
    df["price"] = df["price"].str.replace({',': '', '$': ''}) #It was important to use str
    print(df['price'][0:10])
    
   
    pd.to_numeric(df["price"], downcast='float')
    tableNaming={}
    l=list()
    
    # df1=df[['id','host_id','host_url','host_acceptance_rate','number_of_reviews']]
    # df2=df[['id','latitude','longitude','price']]
    # df3=df[['id','availability_30','availability_60','availability_90']]
    # df4=df[['id','listing_id','date','reviewer_id','reviewer_name','comments']]
    # df5=df[['listing_id','date','available','price','adjusted_price','minimum_nights','maximum_nights']]

   
   
    
    array1=['id','host_id','host_url','host_acceptance_rate','number_of_reviews']
    array2=['id','latitude','longitude','price']
    array3=['id','availability_30','availability_60','availability_90']
    array4=['id','listing_id','date','reviewer_id','reviewer_name','comments']
    array5=['listing_id','date','available','price','adjusted_price','minimum_nights','maximum_nights']
    tableName1='host_informations'
    tableName2='informations_about_accomodations'
    tableName3='avalabilities_general_306090 days'
    tableName4='review'  #nie przechodzi
    tableName5='specificdateavailibities' #nie przechodzi
    #zeby okreslic popularnosc danego noclegu powinienem dodac 
    #the popularity of accomadations i can define for two ways
    #one is that accomodations arent accessibility
    #review_scores_rating
    #reviews_per_month
    #trzeba dodac nowa tabelka gdzie bede okreslal wypelnienie zeby to zrobic to trzeba 30 - availability_30
    add_new_column(array1,df,l,tableName1)
    add_new_column(array2,df,l,tableName2)
    add_new_column(array3,df,l,tableName3)
    add_new_column(array4,df,l,tableName4)
    add_new_column(array5,df,l,tableName5)
  
    
    liczba_wierszy = count_number_of_rows(file)
    print(f'Liczba wierszy w pliku CSV: {liczba_wierszy}')
    chunksize = 100000  
    for keys, chunk in tableNaming.items():
        chunk.to_sql(keys, engine, index=False, if_exists='replace', method='multi')
    print(f"Tabela {file[:-4]} została zapisana w bazie danych.")

engine.dispose()

