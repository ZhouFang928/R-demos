#########################################################
## Tutorial: Big Data Analytics with Microsoft R Server
#########################################################

###########################################################################
## Model Deployment - Hadoop Demo - 10 min 
###########################################################################

## In this session, you will learn how to deploy R models across Hadoop cluster
# with Local, RxHadoopMR & RxSpark compute contexts.
# Let's build a linear regression model using AirlineDemoSmall data.

########## Fetch Data ##########

# Define the HDFS file system

hdfsFS <- RxHdfsFileSystem()

# Set the HDFS location of example data

bigDataDirRoot <- "/example/data"

# Specify the source CSV file to be loaded

source <- system.file("SampleData/AirlineDemoSmall.csv", package = "RevoScaleR")

# Set directory in bigDataDirRoot to load the data into

inputDir <- file.path(bigDataDirRoot, "AirlineDemoSmall")

# Make the directory

rxHadoopMakeDir(inputDir)

# Copy the data from source to input

rxHadoopCopyFromLocal(source, inputDir)

# List files in the input directory

rxHadoopListFiles(inputDir)

########## Format Data ##########

# Create factors for days of the week

colInfo <- list(DayOfWeek = list(type = "factor",
                                 levels = c("Monday", "Tuesday", "Wednesday", "Thursday",
                                            "Friday", "Saturday", "Sunday")))

# Define the text data source in local system

airDSLocal <- RxTextData(file = source, missingValueString = "M",
                         colInfo = colInfo)

# Define the data source in HDFS

airDS <- RxTextData(file = inputDir, missingValueString = "M",
                    colInfo  = colInfo, fileSystem = hdfsFS)

#########################################
####### Set local Compute Context #######

rxSetComputeContext("local")

## Work on the text data source in local system
# Summarize data information (1.4 seconds)

system.time({
  adsSummary <- rxSummary( ~ ArrDelay, data = airDSLocal)
})


# Run a linear regression (2.3 seconds)

system.time({
  modelLocal <- rxLinMod(ArrDelay ~ CRSDepTime + DayOfWeek, data = airDSLocal)
})

## Work on the text data source in HDFS
# Summarize data information (4.9 seconds)

system.time({
  adsSummary <- rxSummary( ~ ArrDelay, data = airDS)
})


# Run a linear regression (4 seconds)

system.time({
  modelLocal <- rxLinMod(ArrDelay ~ CRSDepTime + DayOfWeek, data = airDS)
})

# Display data summary

adsSummary

# Display model summary

summary(modelLocal)

# Make predictions using the regression model (5 seconds)

filename <- file.path(bigDataDirRoot, "AirlineDemoSmallPredictions")
outputXdf <- RxXdfData(filename, fileSystem = hdfsFS, createCompositeSet = TRUE)
system.time({
  predictions <- rxPredict(modelObject = modelHadoop, data = airDS,
                           writeModelVars = TRUE, predVarNames = "ArrDelayExpected",
                           outData = outputXdf, overwrite = TRUE)
})

# Print results (6 seconds)

rxGetInfo(predictions, getVarInfo = TRUE, numRows = 5)

################################################
####### Change Compute Context to Hadoop #######

# Define Hadoop MapReduce compute context

myHadoopCluster <- RxHadoopMR(consoleOutput = TRUE)

# Set compute context

rxSetComputeContext(myHadoopCluster)

# Summarize data information (64 seconds)

system.time({
  adsSummary <- rxSummary( ~ ArrDelay, data = airDS)
})

# Run a linear regression (66 seconds)

system.time({
  modelHadoop <- rxLinMod(ArrDelay ~ CRSDepTime + DayOfWeek, data = airDS)
})

# Display model summary

summary(modelHadoop)

# Make predictions using the regression model (70 seconds)

filename <- file.path(bigDataDirRoot, "AirlineDemoSmallPredictions")
outputXdf <- RxXdfData(filename, fileSystem = hdfsFS, createCompositeSet = TRUE)
system.time({
  predictions <- rxPredict(modelObject = modelHadoop, data = airDS,
                           writeModelVars = TRUE, predVarNames = "ArrDelayExpected",
                           outData = outputXdf, overwrite = TRUE)
})

# Print results (39 seconds)

rxGetInfo(predictions, getVarInfo = TRUE, numRows = 5)

#####################################################
########## Change Compute Context to SPARK ##########

# Define the Spark compute context 

mySparkCluster <- RxSpark(consoleOutput=TRUE)

# Set the compute context 

rxSetComputeContext(mySparkCluster)

# Summarize data information (69 seconds)

system.time({
  adsSummary <- rxSummary( ~ ArrDelay, data = airDS)
})

# Run a linear regression (71 seconds)

system.time({
  modelHadoop <- rxLinMod(ArrDelay ~ CRSDepTime + DayOfWeek, data = airDS)
})

# Display model summary

summary(modelHadoop)

# Make predictions using the regression model (70 seconds)

filename <- file.path(bigDataDirRoot, "AirlineDemoSmallPredictions")
outputXdf <- RxXdfData(filename, fileSystem = hdfsFS, createCompositeSet = TRUE)
system.time({
  predictions <- rxPredict(modelObject = modelHadoop, data = airDS,
                           writeModelVars = TRUE, predVarNames = "ArrDelayExpected",
                           outData = outputXdf, overwrite = TRUE)
})

# Print results (39 seconds)

rxGetInfo(predictions, getVarInfo = TRUE, numRows = 5)

################################################
####### Distribute R code to multiple nodes ######
# test cluster: Run 4 tasks via rxExec

rxExec(function() {Sys.info()["nodename"]}, timesToRun = 4)

#################################
## Questions
#################################

## Why is it slower by using rxHadoopMR & rxSpark compute contexts than by using local compute context?
# What will be the situation if we change the data to a larger dataset? For example, AirOnTimeData in 2012.
# Let's explore it via a Lab.

