#######################################################################
## Tutorial: Big Data Analytics with Microsoft R Server 
## Lab Solution
#######################################################################


# Thanks for completing the labs. Let's check out sample codes.


##############################
## Pre-configuration
##############################

## Install packages

# install.packages("lattice")
# install.packages("gplots")
# install.packages("ggplot2")
# install.packages("rpart")

## Load packages

library(lattice)
library(gplots)
library(ggplot2)
library(rpart)
library(dplyr)
library(magrittr)

library(RevoScaleR)
library(RevoTreeView)

## Set locations
#
# Windows
#
# setwd('C:/Users/dsvmadmin/Documents/Airline')
#
# data.path   <- "C:/Users/dsvmadmin/Documents/Airline/Data"
# output.path <- "C:/Users/dsvmadmin/Documents/Airline/Output/XDF"
#
# Linux
#
# Be sure to set the working directory to that containing this script file.

# setwd("~/demo")

data.path   <- "."
output.path <- "."

if(!file.exists(output.path)) dir.create(output.path, recursive=TRUE)

#########################################
## R & Microsoft R Server - Lab - 5 min
#########################################

## Read the 12 GB of Airline Data in csv format into memory with open source R function. See what happended?
## Write you command below:

######################################
## Data Management - Lab - 10 min
######################################

## Explore the dataset

# Connect to the 3.2 GB of Airline Data in XDF format 'AirOnTime87to12.xdf'.

airlineLab.xdf <- file.path(data.path,"AirOnTime87to12.xdf")

# View the dataset information. (0.18 seconds)

system.time(rxGetInfo(data=airlineLab.xdf, getVarInfo=TRUE) %>% print())

# Summarize the following variables 
# Year, Month, DayOfWeek, UniqueCarrier, ArrDelay, ArrDel15, ArrDelayMinutes, Distance. (15 seconds)

system.time({
	formula= ~ Year + Month + DayOfWeek + UniqueCarrier + ArrDelay + ArrDel15 + ArrDelayMinutes + Distance
	rxSummary(formula=formula, data=airlineLab.xdf) %>% print()
})

# Compute the quartiles of the Arrival Delay column in the data set. (7.8 seconds)

system.time(
    rxQuantile("ArrDelay", airlineLab.xdf) %>% print()
)

# Draw a histogram of ArrDelay (159 seconds)

system.time(
    rxHistogram(~ ArrDelay, airlineLab.xdf)
)

# Count the number of Arrival Delay by Carrier and draw a bar chart (5.25 seconds)

system.time(tmp <- rxCube(ArrDel15 ~ UniqueCarrier, airlineLab.xdf, means=FALSE))
result <- rxResultsDF(tmp) %>% print()
result %>% 
    ggplot(aes(x=UniqueCarrier, y=ArrDel15)) +
    geom_bar(stat="identity", fill="#009E73") +
	xlab("UniqueCarrier") +
	ylab("Count of Arrival Delays") +
	theme_minimal()

# Count the number of Arrival Delay by Month and Carrier and then draw a line chart (7.8 seconds)

system.time(tmp <- rxCube(ArrDel15 ~ F(Month):UniqueCarrier, airlineLab.xdf, means=FALSE))
result <- rxResultsDF(tmp) %>% print()
result %>%
    ggplot(aes(x=Month, y=ArrDel15, group=UniqueCarrier, colour=UniqueCarrier)) + 
    geom_line(size=.75) + 
	geom_point() +
	xlab("Month") +
    ylab("Count of Arrival Delays") +
   	theme_minimal()

## Manipulate the dataset (blocksPerRead=1 321 seconds; blocksPerRead=5 277 seconds; blocksPerRead=10 256 seconds)
# Select the rows where ArrDelay larger than -30 and smaller than 600 and Year equal to 2012.
# Keep the following variables: "Year", "Month", "DayofMonth", "DayOfWeek", 
#			                    "UniqueCarrier", "FlightNum", "Origin", "Dest", "Distance",
#                               "CRSDepTime", "CRSArrTime", "ArrDelay", "ArrDelayMinutes", "ArrDel15".
# Output the data as a new XDF file 'AirOnTime2012.xdf'.
# Check the dataset information again. 

airlineLab_1.xdf <- file.path(output.path,"AirOnTime2012.xdf")
system.time({
    rxDataStep(inData = airlineLab.xdf, 
               outFile = airlineLab_1.xdf, 
			   rowSelection = ArrDelay > -30 & ArrDelay < 600 &
			                  Year ==2012, 
		       varsToKeep =c("Year", "Month", "DayofMonth", "DayOfWeek", 
			                 "UniqueCarrier", "FlightNum", "Origin", "Dest", "Distance",
                             "CRSDepTime", "CRSArrTime", "ArrDelay", "ArrDelayMinutes", "ArrDel15"),
               overwrite=TRUE,
			   blocksPerRead = 10)
})
rxGetInfo(airlineLab_1.xdf, getVarInfo=T, numRows=3)

###############################################
## Predictive Modeling - Lab - 10 min
###############################################

## Build a Glm model on the Airline Data in Year 2012 'AirOnTime2012.xdf' (39 seconds)
# with ArrDelayMinutes as response variable and DayOfWeek, CRSDepTime, Distance as predictors.

system.time({
   glmObj <- rxGlm(formula=ArrDelayMinutes~DayOfWeek + F(CRSDepTime) + Distance, 
                   data = airlineLab_1.xdf, 
				   family = rxTweedie(var.power = 1.15), 
				   blocksPerRead = 5)
})
summary(glmObj)

## Build a Decision Forest model on the Airline Data in Year 2012 'AirOnTime2012.xdf' (50 seconds)
# with ArrDel15 as response variable and Month, DayOfWeek, UniqueCarrier as predictors.
system.time({
   dforestObj <- rxDForest(formula=ArrDel15 ~ Month + DayOfWeek + UniqueCarrier, 
	                       data = airlineLab_1.xdf, 
				           maxDepth=5, 
				           cp = 1.0, 
				           maxSurrogate = 0,
				           blocksPerRead=5)
})
dforestObj
			
## Build a Kmeans clustering model on the Airline Data in Year 2012 'AirOnTime2012.xdf'. (39 seconds)
# with ArrDelay and Distance as variables

airlineKmeans.xdf<-file.path(output.path,"AirOnTime2012_Kmeans.xdf")
system.time({ 
   kmeansObj <- rxKmeans(formula=~ ArrDelay + Distance, 
	                     data = airlineLab_1.xdf,
						 outFile = airlineKmeans.xdf,
		                 numClusters = 10, 
						 algorithm = "lloyd",
						 writeModelVars=TRUE,
						 blocksPerRead = 5)
})
kmeansObj
rxGetInfo(airlineKmeans.xdf, getVarInfo=TRUE, numRows=10)

## Build a Logistic regression model on the Airline Data in Year 1987-2012 'AirOnTime87to12.xdf'. (182 seconds)
# with ArrDel15 as response variable and Month, DayOfweek, UniqueCarrier as predictors. 
system.time({
logitObj <- rxLogit(formula=ArrDel15 ~ Month + DayOfWeek + UniqueCarrier,
                    data = airlineLab.xdf,
                    blocksPerRead=5)
})
summary(logitObj)