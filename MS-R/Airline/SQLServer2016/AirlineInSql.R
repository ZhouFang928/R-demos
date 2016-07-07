#########################################################
## Tutorial: Big Data Analytics with Microsoft R Server
#########################################################

###############################################
## Model Deployment - SQL Demo - 20 min
###############################################

## Through this demo, you will learn 
# How to build R models with In-database analytics functionality provided by SQL Server 2016
# How to deploy and operationalize R models via Transact-SQL stored procedures

############################################################
## Airline Demo with SQL 2016 R Services (Remote Execution)
############################################################

########################################################
###### Step 1 - Specify different compute context ######

sqlConnString <- "Driver=SQL Server;
                  Server=tcp:sql16asianads.southeastasia.cloudapp.azure.com;
                  Database=RevoTestDB;
                  Uid=.;
                  Pwd=."	
sqlCompute<- RxInSqlServer(connectionString = sqlConnString, autoCleanup = FALSE, consoleOutput = TRUE)
localCompute<-RxLocalParallel()

#############################################
###### Step 2 - Load XDF data into SQL ######

tableStart <- Sys.time()

# Set compute context to be LocalParallel()

rxSetComputeContext(localCompute)

# Define XDF data to be loaded

xdfAirDS <- RxXdfData(file.path(rxGetOption("sampleDataDir"),
                                  "AirlineDemoSmall.xdf"))
rxGetVarInfo(xdfAirDS)

# Define SQL Server data source

sqlServerAirDS <- RxSqlServerData(table = "AirlineDemoSmall",
                                    connectionString = sqlConnString)
if (rxSqlServerTableExists("AirlineDemoSmal",
                           connectionString = sqlConnString))
  rxSqlServerDropTable("AirlineDemoSmall", 
                       connectionString = sqlConnString)

# Load XDF data into SQL Server

rxDataStep(inData = xdfAirDS, outFile = sqlServerAirDS, 
           transforms = list(
             DayOfWeek = as.integer(DayOfWeek),
             rowNum = .rxStartRow : (.rxStartRow + .rxNumRows - 1)
           ), overwrite = TRUE
)

############################################################
###### Step 3 - Connect to the SQL Server data source ######

# Switch compute context to In-SQL

rxSetComputeContext(sqlCompute)

# Connect to the SQL data source

SqlServerAirDS <- RxSqlServerData(sqlQuery = 
	"SELECT * FROM AirlineDemoSmall",
	connectionString = sqlConnString,
      rowsPerRead = 50000,
	colInfo = list(DayOfWeek = list(type = "factor", 
		levels = as.character(1:7))))

# View the data information

rxGetInfo(SqlServerAirDS,getVarInfo=T,numRows=3)

# Summarize the data

rxSummary(~., data = SqlServerAirDS)	

######################################
###### Step 4 - Train the model ######

# Build a linear regression model

linear_model<-rxLinMod(formula=ArrDelay~CRSDepTime+DayOfWeek, data=SqlServerAirDS)
print(linear_model)

######################################
###### Step 5 - Score the model ######

# Switch back to local compute context

rxSetComputeContext(localCompute)

# Prepare the test dataset

AirDS <- rxImport(inData=SqlServerAirDS)

# Make prediction
prediction <- rxPredict(modelObject = linear_model,
                         data = AirDS,
                         overwrite = TRUE)
head(prediction)
AirPred<- cbind(AirDS[c(4,3,2,1)], prediction$ArrDelay_Pred)
colnames(AirPred) <- c("FlightNum", "DayOfWeek", "CRSDepTime", "ArrDelayActual", "ArrDelayExpected")
head(AirPred)
					
