spark = SparkSession.builder.appName("AQIandCensus").getOrCreate()
path = "C:/Users/madhu/Documents/git/Udacity/DEND/Capstone/data/epa/daily_42101_2019.csv"
raw_data_42101 = spark.read.csv(path, header='true', inferSchema='true')
raw_data_42101.printSchema()
raw_data_42101.count()
raw_data_42101.select("Parameter Name").dropDuplicates().show()
raw_data_42101.describe("AQI").show()
raw_data_42101.select("AQI").dropDuplicates().show()
raw_data_42101.where(raw_data_42101.AQI.isNull()).count()
raw_data_42101.where(raw_data_42101["Arithmetic Mean"].isNull()).count()
raw_data_42101.describe("Arithmetic Mean").show()
data_CO = raw_data_42101.select("State Code", "County Code", "Site Num", "POC", "Sample Duration", "Date Local", "Arithmetic Mean", "1st Max Value", "AQI", "State Name", "County Name")
data_CO.colNames = ["State_Code", "Count_Code", "Site_Num", "POC", "Sample_Duration", "Date_Local", "Mean", "Max_Value", "AQI", "State_Name", "County_Name"]
data_CO.describe()