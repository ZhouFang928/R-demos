#########################################################
## Tutorial: Big Data Analytics with Microsoft R Server
#########################################################

#######################################################################################
## Model Deployment - Hadoop Lab - 18 min (not include the time for data downloading)
######################################################################################

## In this session, you will practice on deploying R models across Hadoop cluster
# with Local, RxHadoopMR & RxSpark compute contexts.
# Let's build a logistic regression model using AirOnTime data in 2012.

########## Fetch Data ##########

# Define the HDFS file system

hdfsFS <- RxHdfsFileSystem()

# Set the HDFS location of example data

bigDataDirRoot <- "/example/data"

# Create a local folder for storaging data temporarily

source <- "/tmp/AirOnTimeCSV2012"
dir.create(source)

# Download data to the tmp folder

remoteDir <- "http://packages.revolutionanalytics.com/datasets/AirOnTimeCSV2012"
download.file(file.path(remoteDir, "airOT201201.csv"), file.path(source, "airOT201201.csv"))
download.file(file.path(remoteDir, "airOT201202.csv"), file.path(source, "airOT201202.csv"))
download.file(file.path(remoteDir, "airOT201203.csv"), file.path(source, "airOT201203.csv"))
download.file(file.path(remoteDir, "airOT201204.csv"), file.path(source, "airOT201204.csv"))
download.file(file.path(remoteDir, "airOT201205.csv"), file.path(source, "airOT201205.csv"))
download.file(file.path(remoteDir, "airOT201206.csv"), file.path(source, "airOT201206.csv"))
download.file(file.path(remoteDir, "airOT201207.csv"), file.path(source, "airOT201207.csv"))
download.file(file.path(remoteDir, "airOT201208.csv"), file.path(source, "airOT201208.csv"))
download.file(file.path(remoteDir, "airOT201209.csv"), file.path(source, "airOT201209.csv"))
download.file(file.path(remoteDir, "airOT201210.csv"), file.path(source, "airOT201210.csv"))
download.file(file.path(remoteDir, "airOT201211.csv"), file.path(source, "airOT201211.csv"))
download.file(file.path(remoteDir, "airOT201212.csv"), file.path(source, "airOT201212.csv"))

# Set directory in bigDataDirRoot to load the data into

inputDir <- file.path(bigDataDirRoot, "AirOnTimeCSV2012")

# Make the directory

rxHadoopMakeDir(inputDir)

# Copy the data from source to input

rxHadoopCopyFromLocal(source, bigDataDirRoot)

# List files copied onto Hadoop

rxHadoopListFiles(inputDir)

########## Format Data ##########

# Create info list for the airline data

airlineColInfo <- list(
    DAY_OF_WEEK = list(type = "factor"),
    DISTANCE = list(type = "integer"),
    DEP_TIME = list(type = "integer"),
    ARR_DEL15 = list(type = "logical"))

# Get all the column names

varNames <- names(airlineColInfo)

# Define the text data source in HDFS

airOnTimeData <- RxTextData(inputDir, colInfo = airlineColInfo, varsToKeep = varNames, fileSystem = hdfsFS)

##########################################
####### Set Local Compute Context #######

# Set the compute context

rxSetComputeContext("local")

# Compute summary information on the CSV data in HDFS 'airOnTimeData' (200 seconds)

system.time({
  adsSummary <- rxSummary(~ARR_DEL15 + DISTANCE + DEP_TIME + DAY_OF_WEEK, data = airOnTimeData)
})
adsSummary

##########################################
####### Set Hadoop Compute Context #######

# Specify Hadoop Map Reduce compute context

myHadoopCluster <- RxHadoopMR(consoleOutput=TRUE)

# Set the compute context

rxSetComputeContext(myHadoopCluster)

# Compute summary information on the CSV data in HDFS 'airOnTimeData' (139 seconds)

system.time({
  adsSummary <- rxSummary(~ARR_DEL15 + DISTANCE + DEP_TIME + DAY_OF_WEEK, data = airOnTimeData)
})
adsSummary

# Define XDF file and import from CSV data in HDFS (97 seconds)

filename <- file.path(bigDataDirRoot, "AirOnTimeData")
airOnTimeDataXdf <- RxXdfData(filename, fileSystem = hdfsFS )
system.time({
  rxImport(inData = airOnTimeData, outFile = airOnTimeDataXdf,
           rowsPerRead = 250000, overwrite = TRUE, numRows = -1)
})
rxGetInfo(airOnTimeDataXdf, getVarInfo=T)

# NOTE: From now on, all computation will be done on the XDF file

# Compute summary information on the XDF file 'airOnTimeDataXdf' (76 seconds)

system.time({
adsSummary <- rxSummary(~ARR_DEL15 + DISTANCE + DEP_TIME + DAY_OF_WEEK, data = airOnTimeDataXdf)
})
adsSummary

########## Build Logistic Regression Model with Hadoop ##########

# Formula to use
formula <- "ARR_DEL15 ~ DISTANCE + DAY_OF_WEEK + DEP_TIME"

# Run a logistic regression (259 seconds)

system.time({
modelHadoop <- rxLogit(formula, data = airOnTimeDataXdf)
})

# Display model summary
summary(modelHadoop)

########## Make Predictions with Hadoop ########## 

# Make predictions using the logit model (74 seconds)

filename <- file.path(bigDataDirRoot, "AirOnTimePredictions")
outputXdf <- RxXdfData(filename, fileSystem = hdfsFS, createCompositeSet = TRUE)
system.time({
predictions <- rxPredict(modelObject = modelHadoop, data = airOnTimeDataXdf,
			writeModelVars = TRUE, predVarNames = "Score",
			outData = outputXdf, overwrite = TRUE)
})

# Print results
system.time({
rxGetInfo(predictions, getVarInfo = TRUE, numRows = 5)
})

#####################################################
########## Change Compute Context to SPARK ##########

# Define the Spark compute context 

mySparkCluster <- RxSpark(consoleOutput=TRUE)

# Set the compute context 

rxSetComputeContext(mySparkCluster)

########## Build Model with SPARK ##########

# Formula to use
formula <- "ARR_DEL15 ~ DISTANCE + DAY_OF_WEEK + DEP_TIME"

# Run a logistic regression (108 seconds)

system.time({
modelSpark <- rxLogit(formula, data = airOnTimeDataXdf)
})

# Display model summary

summary(modelSpark)

########## Make Predictions with Spark ##########

# Make predictions using the logit model (77 seconds)

filename <- file.path(bigDataDirRoot, "AirOnTimePredictions")
outputXdf <- RxXdfData(filename, fileSystem = hdfsFS, createCompositeSet = TRUE)
system.time({
predictions <- rxPredict(modelObject = modelSpark, data = airOnTimeDataXdf,
			writeModelVars = TRUE, predVarNames = "Score",
			outData = outputXdf, overwrite = TRUE)
})

# Print results

system.time({
rxGetInfo(predictions, getVarInfo = TRUE, numRows = 5)
})