#########################################################
## Tutorial: Big Data Analytics with Microsoft R Server
#########################################################


# Welcome! This tutorial is intended to help you learn the 
# essentials of Microsoft R Server: its strengths, when to use it, and how to 
# use its core functions. If you need help at any time or have questions that
# aren't answered here, feel free to ask a proctor.


##############################
## TIPS
##############################


# 1. Press Ctrl+Enter to run the current line of code, or a selection.
# 2. Output will print to the console. You'll have to scroll up to see it all.
# 3. Get help for any function by typing a question mark and then its name into
#    the console: ?rxLinMod
# 4. Get help for *all* the MRS functions with: help("RevoScaleR")
# 5. Files will appear in the "Files" pane as you create them.
# 6. R objects will appear in the "Environment" pane as you create them.
# 7. Run all the example code
# 8. If you get a "file not found" error, click Session -> Set Working 
#    Directory -> To Source File Location, and check the "Files" pane to be sure
#    the file in question exists.
# 9. Ask for help if you need it!


##############################
## Pre-configuration
##############################


## Install packages
install.packages("gplots")
install.packages("ggplot2")
install.packages("rpart")

## Load packages
library("gplots")
library("ggplot2")
library("RevoTreeView")
library("rpart")

## Set directory
setwd('C:/Users/dsvmadmin/Documents/Airline')
data.path <- "C:/Users/dsvmadmin/Documents/Airline/Data"
output.path <- "C:/Users/dsvmadmin/Documents/Airline/Output/XDF"
if(!file.exists(output.path)) dir.create(output.path, recursive=TRUE)


##############################################
## R & Microsoft R Server - Mini Demo - 10 min
##############################################


## Analyze with open source R functions
# Define a colInfo vector
airline.csv <- file.path(data.path,"2007.csv")
myColInfo = rep("NULL", 29)
myColInfo[c(12, 14, 15, 16, 19)] <- "numeric"

# Build a data frame
system.time(airline.df <- read.csv(file=airline.csv, header = TRUE, colClasses=myColInfo))

# Show data structure
system.time(str(airline.df))

# Summarize data
system.time(curSum  <- summary(airline.df))
curSum

# Create new variable AirSpeed
airline.df$AirSpeed<-(airline.df$Distance/airline.df$AirTime)*60

# View data information of the new data
str(airline.df)

# Check normal assumption and Draw histogram
hist(x=airline.df$AirSpeed,col="lightblue",main=NULL,xlab="AirSpeed")
airline.subset<-subset(airline.df, AirSpeed>50 & AirSpeed<800)
system.time({hist(x=airline.subset$AirSpeed,col="lightblue",main=NULL,xlab="AirSpeed",breaks=100)
axis(1,at=seq(50,800,by=50),tick=TRUE,labels=TRUE)})

# Build linear regression model 
system.time(airline.model<-lm(AirSpeed~DepDelay,data=airline.subset))
summary(airline.model)

##Setting Options for Microsoft R Server
?rxOptions        # for setting global options for Revolution's global environment
rxOptions()  

## Analyze with RevoScaleR provided by Microsoft R Server
# Set to RxLocalParallel Compute Context
#rxOptions(computeContext=RxLocalSeq())
rxOptions(computeContext=RxLocalParallel())

# Read data
airline.xdf <- file.path(output.path,"2007.xdf")
system.time(rxImport(inData=airline.csv, outFile = airline.xdf,varsToKeep=c("ActualElapsedTime","AirTime","ArrDelay","DepDelay","Distance"),overwrite = TRUE))
# NOTE: A dataset has been created for you, all Airline flights in the US for the year 2007.

# View data info
system.time(curInfo <- rxGetInfo(data=airline.xdf, getVarInfo=TRUE))
curInfo

# Summarize data
system.time(curSum <- rxSummary(~ActualElapsedTime + AirTime + ArrDelay+DepDelay + Distance, 
          data=airline.xdf))
curSum

# Create new variable AirSpeed
system.time(
rxDataStep(inData=airline.xdf, 
           outFile=airline.xdf, 
           transforms = list(AirSpeed = Distance / AirTime * 60), 
           append="cols", 
           overwrite=TRUE)
)

# View updated data info
rxGetInfo(data=airline.xdf, getVarInfo=TRUE)

# Check normal assumption and Draw histogram
rxHistogram(~AirSpeed, data=airline.xdf)
rxHistogram(~AirSpeed, data=airline.xdf, 
            rowSelection=(AirSpeed>50) & (AirSpeed<800), 
            numBreaks=5000,
            xNumTicks=20)
		
# Build linear regression model
system.time({
  airline.model <- rxLinMod(formula=AirSpeed~DepDelay, data=airline.xdf, 
                            rowSelection=(AirSpeed>50) & (AirSpeed <800))
})
summary(airline.model)

# NOTE: Please check the recorded processing time of open source R functions and RevoScaleR functions from the Mini Demo above.


#########################################
## R & Microsoft R Server - Lab - 5 min
#########################################


## Read the 12 GB of Airline Data in csv format into memory with open source R function. See what happended?
## Write you command below:




##########################################
## Data Management - Manipulation - 5 min
##########################################


## rxImport - Import the full airline.csv data source and save it as a XDF file
rxImport(inData=airline.csv, outFile = airline.xdf, overwrite = TRUE)

## rxGetInfo - View data information
rxGetInfo(airline.xdf,getVarInfo=T,numRows=3)

## rxFactor - Manipulate data by coercing some variables as factors/categorical variables
airline1.xdf<-file.path(output.path,"airline1.xdf")
rxFactors(inData=airline.xdf, outFile=airline1.xdf,
          factorInfo=list(Origin=list(sortLevels=TRUE),
                          Dest=list(sortLevels=TRUE),
                          TailNum=list(sortLevels=TRUE),
                          UniqueCarrier=list(sortLevels=TRUE),
                          Month=list(newLevels=c("Jan","Feb","Mar","Apr","May","Jun",
                                                 "Jul","Aug","Sep","Oct","Nov","Dec"),
                                     levels=c("1","2","3","4","5","6","7","8","9","10","11","12"),
                                     sortLevels=FALSE),
                          DayOfWeek=list(newLevels=c("Mon","Tue","Wed","Thur","Fri","Sat","Sun"),
                                         levels=c("1","2","3","4","5","6","7"),
                                         sortLevels=FALSE)
                          ),
          overwrite=TRUE)
rxGetInfo(airline1.xdf,getVarInfo=T,numRows=3)

## rxDataStep - Manipulate data by removing missing value, transforming variable format, select/delete/create variables
airline2.xdf<-file.path(output.path,"airline2.xdf")
rxDataStep(inData = airline1.xdf, outFile = airline2.xdf, 
	       removeMissings=TRUE,
		   varsToDrop = c("ActualElapsedTime", "CRSElapsedTime", "AirTime", "CarrierDelay", 
			              "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay"),
           transforms = list(FlightNum = as.character(FlightNum),
			                 IsADelay = ifelse(ArrDelay>15,1,0)),
           overwrite=TRUE)
rxGetInfo(airline2.xdf,getVarInfo=T,numRows=3)


########################################################
## Data Management - Exploration & Visualization - 5 min
########################################################


## rxHistogram - Draw histograms
rxHistogram(~ ArrDelay, airline2.xdf)

# Notice quite a number of rare events, ie Huge Delays or Huge Early Arrivals.
# These could be strange events, comment them out
rxHistogram(~ ArrDelay, airline2.xdf, rowSelection= ArrDelay > -30 & ArrDelay < 600)

# NOTE: Which gives a more accurate picture of how the distribution of Arrival Delays look like. 
# Non-gaussian, and long-tailed.
# What type of model would you use to model such behaviour?
# Will simple linear regression model work? 
# Actually, the answer is no, because the gaussian assumption for linear regression model is not satisfied.
# In the later session, we create a new dummy variable and build a logistic regression model

# We can look at the distribution by months, and see if there is any change
rxHistogram(~ArrDelay|Month, airline2.xdf, 
            rowSelection= ArrDelay > -30 & ArrDelay < 600)

## rxCrossTab & rxCube - Aggregate data

# What's the average Arrival Delay by Month and Carrier
rxCrossTabs(ArrDelay ~ UniqueCarrier:Month, airline2.xdf, means=T)
rxCrossTabs( ~ UniqueCarrier:Month, airline2.xdf)

# What's the average Arrival Delay by Month and Carrier
rxCube(ArrDelay~UniqueCarrier:Month, airline2.xdf)
rxCube(~UniqueCarrier:Month, airline2.xdf)

# NOTE: Please find out the difference between rxCrossTabs and rxCube

## rxCrossTabs/rxCube + R packages for visualization 

# Draw HeatMap of Average Arrival Delays by Month and Carrier 
tmp1 <- rxCrossTabs(ArrDelay ~ UniqueCarrier:Month, airline2.xdf,means=T)
result <- rxResultsDF(tmp1,output="means")

mySub <- apply(result,1,function(x) sum(is.na(x)))
result <- result[mySub != 12,]
colnames(result)[-1] -> pp
substring(pp,7)->pp
colnames(result)[-1] <- pp

library(gplots)
par(oma=c(2,2,2,2))
heatmap.2(data.matrix(result[,-1]), 
          Colv=FALSE,labRow=result[,1],
          col=rev(heat.colors(16)),trace='none',symkey=FALSE,key.ylab="Mean")

# Draw Line Plot of Average Arrival Delays by Carrier and DayOfWeek
tmp2<-rxCube(ArrDelay~UniqueCarrier:DayOfWeek, airline2.xdf)
result <- rxResultsDF(tmp2)
ggplot(result, aes(x = DayOfWeek, y = ArrDelay, group = UniqueCarrier, colour = UniqueCarrier)) + 
geom_line(size=.75) + 
geom_point() + 
xlab("Weekday") + 
ylab("Average of Arrival Delays") + 
theme_minimal()


######################################
## Data Management - Lab - 10 min
######################################


## Connect to the 12 GB of Airline Data in XDF format (file.path)
## View data information (rxGetInfo)
## Summarize data (rxSummary)
## Draw histogram to view the distribution of ArrDelay (rxHistogram)
## Create a new variable ArrDel15 taking value 1 when ArrDelay > 15 and taking value 0 when ArrDelay < 15 (rxDataStep)
## Count the number of Arrival Delay by Carrier and draw a bar chart (rxCube & ggplot)
## Count the number of Arrival Delay by Month and Carrier and then draw a line chart (rxCube & ggplot)




#################################################
## Predictive Modeling with PEMAs - Demo - 10 min
#################################################


## Before modeling
## Data filtering 
airlineTrainTest.xdf<-file.path(output.path,"airlineTrainTest.xdf")
rxDataStep(airline2.xdf,outFile=airlineTrainTest.xdf,
           rowSelection = !Month %in% c('September','October','November','December') & 
              ArrDelay > -30 & ArrDelay < 600 &
              Origin %in% c('HNL','TPA','MEM','MIA','HOU','JFK','EWR','LAX','SFO','ORD','ATL','AUS'),
           transforms=list(IsADelay = ifelse(ArrDelay>15,1,0),
                           urv=factor(ifelse(runif(length(Year))<0.8,'TRAIN','TEST'))),
           overwrite=T)
rxGetInfo(airlineTrainTest.xdf,T,numRows=3)

## Split data into training/testing data set
train.xdf <- file.path(output.path,'airlineTrainTest.urv.TRAIN.xdf')
test.xdf <- file.path(output.path,'airlineTrainTest.urv.TEST.xdf')
rxSplit(airlineTrainTest.xdf,outFilesBase=airlineTrainTest.xdf,splitByFactor='urv',overwrite=T)

######################
## Logistic Regression
#######################

## Build Logistic Regression model
r1 <- rxLogit(IsADelay ~ Month + DayOfWeek + UniqueCarrier,train.xdf,blocksPerRead=5)
summary(r1)

## Predict Logistic Model on our test dataset
rxPredict(r1,data=test.xdf,computeResiduals=T,predVarNames='LogitPredict',overwrite=TRUE)
rxGetInfo(test.xdf,getVarInfo=TRUE,numRows=10)

## Draw a ROC curve
rxRocCurve(actualVarName='IsADelay',predVarNames='LogitPredict',data=test.xdf)


## Include Major Airports to see whether it helps to improve the model accuracy
r2 <- rxLogit(IsADelay ~ Month + DayOfWeek + UniqueCarrier + Origin, 
              train.xdf,blocksPerRead=5)
rxPredict(r2, data=test.xdf,computeResiduals=T,predVarNames='LogitWithOrigin',overwrite=TRUE)
rxRocCurve(actualVarName='IsADelay',predVarNames='LogitWithOrigin',data=test.xdf)

##################
## Decision Tree
##################

## Build Decision Tree model
d1 <- rxDTree(IsADelay ~ Month + DayOfWeek + UniqueCarrier + Origin, train.xdf, maxDepth=5, blocksPerRead=5)
d1

## View Decision Tree
# View 1
library(RevoTreeView)
plot(createTreeView(d1)) 
# View 2
library(rpart)
plot(rxAddInheritance(d1))
text(rxAddInheritance(d1))

## Prediction
rxPredict(d1, data=test.xdf,computeResiduals=T,predVarNames='DTreeWithOrigin',overwrite=TRUE)

## Model evaluation
rxRocCurve(actualVarName='IsADelay',predVarNames=c('DTreeWithOrigin','LogitWithOrigin'),data=test.xdf)


###############################################
## Predictive Modeling - Lab - 10 min
###############################################


## Build a Decision Forest model on the 670 MB of Airline Data [flight on-time performance data in 2007] (rxDForest)
## Build a Decision Forest model on the 12 GB of Airline Data [flight on-time performanfe data from 1987 to 2008] (rxDForest, blocksPerRead)
## ?rxDForest



###############################################
## Model Deployment - Demo - 20 min
###############################################


## See the demonstration
## Try it out at home
# 1. Set up a SQL 2016 Server virtual machine using your 200 USD free azure subscription
# 2. Load the data '2007.csv' into your database
# 3. Execute R script 'AirlineInSql.R' from remote client
# 4. Execute sql scripts in the folder 'SQLR' using SSMS

