import pandas  as pd
from pyspark.sql import SparkSession\

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.options.display.float_format = '{:.2f}'.format

def create_spark_session():
    ''' Creates spark session '''
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def raw_data_probe_sprk(spark):
     mainDF_i94=spark.read.parquet("/home/workspace/sas_data")
     mainDF_i94.columns
     mainDF_i94.show(2)

     mainDF_airport=spark.read.csv("/home/workspace/airport-codes_csv.csv",inferSchema=True, header=True)
     mainDF_airport.columns
     mainDF_airport[['iata_code']].show()

     mainDF_dem=spark.read.csv("/home/workspace/us-cities-demographics.csv",inferSchema=True, header=True, sep=';')
     mainDF_dem.columns
     mainDF_dem.show()

def raw_data_probe_py():
     pdDF_i94=pd.read_parquet("/home/workspace/sas_data", engine='fastparquet')

     pdDF_i94['dtadfile_date']=pd.to_datetime(pdDF_i94['dtadfile'], format='%Y/%m/%d', errors='coerce')
     pdDF_i94['dtaddto_date']=pd.to_datetime(pdDF_i94['dtaddto'],format='%Y/%m/%d', errors='coerce')
     #pdDF_i94['arrive_date']=pd.to_datetime(pdDF_i94['arrdate'], errors='coerce')
     #pdDF_i94['dep_date']=pd.to_datetime(pdDF_i94['depdate'], errors='coerce')
     print('$$$$$$$')
     print(pdDF_i94['dtadfile_date'].min())
     print(pdDF_i94['dtadfile_date'].max())
     print(pdDF_i94['dtaddto_date'].min())
     print(pdDF_i94['dtaddto_date'].max())
     print(pdDF_i94['i94cit'].drop_duplicates().count())
     print(pdDF_i94.groupby('gender').size())
     print('$$$$$$$')

     df = pd.read_sas('/home/workspace/I94_SAS_Labels_Descriptions.SAS',format='sas7bdat')
     print(df)

     #print(pdDF_i94)
     #print(pdDF_i94.describe())
     #print(pdDF_i94.dtypes)
     #print(pdDF_i94[['arrdate','depdate']].dtypes)
 
'''
     print("**********************************")

     pdDF_airport=pd.read_csv("/home/workspace/airport-codes_csv.csv")
     print(pdDF_airport[['ident','iata_code']].dropna().count())
     print(pdDF_airport.describe())
     #print(pdDF_airport.columns)
     print(pdDF_airport.dtypes)
     print("**********************************")


     pdDF_dem=pd.read_csv("/home/workspace/us-cities-demographics.csv", sep=';')
     print(pdDF_dem)
     print(pdDF_dem['Total Population'].apply(lambda x: '{:d}'.format(x) ).describe())
     #print(pdDF_temp.columns)
     print(pdDF_dem.dtypes)
     print("**********************************")
'''

def main():

    print("Begin ETL program...")

    spark = create_spark_session()

    #raw_data_probe_sprk(spark)
    #raw_data_probe_py()
    #from pyspark.sql import SparkSession
    #spark = SparkSession.builder.config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11").enableHiveSupport().getOrCreate()
    #df_spark =spark.read.format('com.github.saurfang.sas.spark').load('I94_SAS_Labels_Descriptions.SAS')

    print("End ETL program...")


if __name__ == "__main__":

    main()
