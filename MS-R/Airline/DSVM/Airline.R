#########################################################
## Tutorial: Big Data Analytics with Microsoft R Server
#########################################################
#
# Welcome! This tutorial is intended to help you learn the essentials
# of Microsoft R Server: its strengths, when to use it, and how to use
# its core functions. If you need help at any time or have questions
# that aren't answered here, feel free to ask a proctor.
#
# A dataset has been created for you. It records all airline flights
# in the US for the year 2007.
#
# Change log:
#
# 160707 gjw Normalise variable names.
# 160704 gjw Move to conform to style guide and cleanup for Linux and
# 160701 zf develop the original script
# Emacs ESS as well as RStudio.

##############################
## TIPS
##############################
#
# 1. Press Ctrl+Enter to run the current line of code, or a selection.
#
# 2. Output will print to the console. You'll have to scroll up to see
#    it all.
#
# 3. Get help for any function by typing a question mark and then its name into
#    the console: ?rxLinMod
#
# 4. Get help for *all* the MRS functions with: help("RevoScaleR")
#
# 5. Files will appear in the "Files" pane of RStudio as you create
#    them.
#
# 6. R objects will appear in the "Environment" pane of RStudio as you
# create them.
#
# 7. Run all the example code
#
# 8. If you get a "file not found" error, click Session -> Set Working
#    Directory -> To Source File Location, (or ensure correct working
#    directory on starting up ESS) and check the "Files" pane in
#    RStudio to be sure the file in question exists.
#
# 9. Ask for help if you need it!

##############################
## Pre-configuration
##############################

## Clean up first off and start from a known random place.

rm(list = ls())
set.seed(42)

## Required packages.

packages <- c(
  "dplyr",    # Data wrangling.
  "ggplot2",  # Visualise data.
  "gplots",   # Heatmap.
  "lattice",  # Nicer plots from Revo.
  "magrittr", # Pipelines for data processing: %>% %T>% %<>%.
  "rattle",   # Wrangling normVarNames().
  "readr",    # Faster read_csv().
  "rpart",    # Build decision trees.
  "devtools"  # Development tools
)

for (package in packages)
{
  if (!require(package, character.only=TRUE))
  {
    install.packages(package, character.only=TRUE)
    library(package, character.only=TRUE)
  }
}

# Load the Microsoft R Server functionality.

library(RevoScaleR)
library(RevoTreeView)
library(RevoPemaR)

# We need to grab Hong's dplyrXdf from github using devtools to
# install.

if (!require("dplyrXdf", character.only=TRUE))
{
  devtools::install_github("Hong-Revo/dplyrXdf")
  library("dplyrXdf", character.only=TRUE)
}

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

data.path   <- "data"
output.path <- "output/xdf"

if(!file.exists(output.path)) dir.create(output.path, recursive=TRUE)

##############################################
## R Mini Demo - 10 min
##############################################

## Analyze with open source R functions.

# Fix the supplied CSV dataset to use normalised variable names.

fstem <- "2007"
airline.csv <- file.path(data.path, paste0(fstem, ".csv")) %T>% print()

system.time( # read.csv() 80s F4; readr::read_csv() 20s F4.
  airline <- read_csv(airline.csv)
)

names(airline) %<>% normVarNames()

airline.csv <- file.path(data.path, paste0(fstem, "norm.csv")) %T>% print()

system.time( # 40s F4.
  write_csv(airline, airline.csv)
)

# Keep just a few columns for now and use a generic variable to store
# the dataset.

keep.vars   <- c("actual_elapsed_time", "air_time",
                 "arr_delay", "dep_delay", "distance")

ds <- airline[keep.vars]

# Show the dataset structure.

system.time( # 0.02s
    ds %>% str()
)

# Summarize the dataset.

system.time( # 13s A3; 2s F4.
    summary(ds) %>% print()
)

# Create new variable air_speed.

ds %<>% mutate(air_speed=60*distance/air_time)

# View information of the new dataset.

str(ds)

# Check assumption of normal distribution with a histogram.

hist(x=ds$air_speed, col="lightblue", main=NULL, xlab="Air Speed")

# Using ggplot2 takes quite a bit longer.
#
# ds %>% ggplot(aes(x=air_speed)) + geom_histogram(bins=100)

# We will use a subset with well defined values (remove infinite and
# missing values).

ds %<>% subset(air_speed>50 & air_speed<800)

system.time( # 0.4s F4.
{
  hist(x=ds$air_speed, col="lightblue", main=NULL, xlab="air_speed", breaks=100)
  axis(1, at=seq(50, 800, by=50), tick=TRUE, labels=TRUE)
})

# Build linear regression model.

system.time( # 46-22s A3; 13-8s F4
    model <- lm(air_speed ~ dep_delay, data=ds)
)
summary(model)

##############################################
## Microsoft R Server Mini Demo - 10 min
##############################################

## Now on to Microsoft R Server using RevoScaleR package.
#
# Let's first check the global "Revolution" Options for Microsoft R Server.

# ?rxOptions
rxOptions()
rxOptions()$computeContext
rxOptions()$numCoresToUse

# Choose to use the LocalParallel() compute context.

rxOptions(computeContext=RxLocalParallel())$computeContext

# An alternative would be to use a sequential compute context.
#
# rxOptions(computeContext=RxLocalSeq())

# Create an XDF data format by importing the CSV data.
#
# Note that this is done sequentially not in parallel. We can see this
# by running htop in a terminal.

airline.xdf <- file.path(output.path, paste0(fstem, "norm.xdf")) %T>% print()

system.time( # 4m A3; 1m F4.
    rxImport(inData=airline.csv,
             outFile=airline.xdf, 
             varsToKeep=keep.vars,
             overwrite=TRUE)
)

# View the dataset information.

system.time( # 0.02s F4.
    rxGetInfo(data=airline.xdf, getVarInfo=TRUE) %>% print()
)

# Summarize the dataset.

system.time( # 1s F4.
    rxSummary(~., data=airline.xdf) %>% print()
)

# Create new variable air_speed.

system.time( # 5s F4.
    rxDataStep(inData=airline.xdf, 
               outFile=airline.xdf, 
               transforms=list(air_speed=60*distance/air_time), 
               append="cols", 
               overwrite=TRUE)
)

# View updated dataset information.

rxGetInfo(data=airline.xdf, getVarInfo=TRUE)

# Check assumption of normal distribution with a histogram.

rxHistogram(~air_speed, data=airline.xdf)

rxHistogram(~air_speed,
            data=airline.xdf, 
            rowSelection=(air_speed>50) & (air_speed<800), 
            numBreaks=5000,
            xNumTicks=20)
		
# Build linear regression model.

system.time( # 4s A3; 1s F4.
  model <- rxLinMod(formula=air_speed~dep_delay, 
                    data=airline.xdf, 
                    rowSelection=(air_speed>50) & (air_speed <800))
)
summary(model)

# NOTE: Please record the processing time for each of the open source R 
# and RevoScaleR functions from the above.

#########################################
## R & Microsoft R Server - Lab - 5 min
#########################################

## If we try to read 12 GB of Airline Data in csv format into memory
## with open source R it will generally run out of memory.  Try it
## yourself sometime.

## Please read the file 'AirOnTime87to12.dataset.description.txt' to
## check data information and see how the XDF file 'AirOnTime87to12.xdf'
## is generated.

##########################################
## Data Management - Manipulation - 5 min
##########################################

## rxImport() - Import the full airline.csv data source and save it as
## a XDF file.

system.time( # 2m F4.
    rxImport(inData=airline.csv, outFile=airline.xdf, overwrite=TRUE)
)

## rxGetInfo() - View dataset information.

rxGetInfo(airline.xdf, getVarInfo=TRUE, numRows=3)

## rxFactor() - Manipulate data by coercing some variables as
## factors/categorical variables

airline1.xdf <- file.path(output.path, paste0(fstem, "norm1.xdf")) %T>% print()

rxFactors(inData=airline.xdf,
          outFile=airline1.xdf,
          factorInfo=list(origin=list(sortLevels=TRUE),
                          dest=list(sortLevels=TRUE),
                          tail_num=list(sortLevels=TRUE),
                          unique_carrier=list(sortLevels=TRUE),
                          month=list(newLevels=c("Jan", "Feb", "Mar", "Apr",
                                                 "May", "Jun", "Jul", "Aug",
                                                 "Sep", "Oct", "Nov", "Dec"),
                                     levels=c("1", "2", "3", "4", "5", "6",
                                              "7", "8", "9", "10", "11", "12"),
                                     sortLevels=FALSE),
                          day_of_week=list(newLevels=c("Mon", "Tue", "Wed",
                                                       "Thur", "Fri", "Sat", "Sun"),
                                           levels=c("1", "2", "3",
                                                    "4", "5", "6", "7"),
                                           sortLevels=FALSE)
                          ),
          overwrite=TRUE)

rxGetInfo(airline1.xdf, getVarInfo=TRUE, numRows=3)

## rxDataStep() - Manipulate data by removing missing values,
## transforming variable formats, select/delete/create variables.

airline2.xdf<-file.path(output.path, paste0(fstem, "norm2.xdf")) %T>% print()

rxDataStep(inData=airline1.xdf,
           outFile=airline2.xdf, 
           removeMissings=TRUE,
           varsToDrop=c("actual_elapsed_time", "crselapsed_time", "air_time",
                        "carrier_delay", "weather_delay", "nasdelay",
                        "security_delay", "late_aircraft_delay"),
           transforms=list(flight_num=as.character(flight_num),
                           isa_delay=ifelse(arr_delay>15,1,0)),
           overwrite=TRUE)

rxGetInfo(airline2.xdf, getVarInfo=TRUE, numRows=3)

########################################################
## Data Management - Exploration & Visualization - 5 min
########################################################

## rxHistogram() - Draw histograms.

rxHistogram(~ arr_delay, airline2.xdf)

# Notice quite a number of rare events, i.e., huge delays or huge
# early arrivals.  These could be outlier events so we ignore them
# from our analysis.

rxHistogram(~ arr_delay, airline2.xdf, rowSelection=arr_delay>-30 & arr_delay<600)

# NOTE: Which gives a more accurate picture of how the distribution of
# Arrival Delays look like?
#
# Non-gaussian and long-tailed.
#
# What type of model would you use to model such behaviour?
#
# Will simple linear regression model work?
#
# Actually, the answer is no, because the gaussian assumption for
# linear regression model is not satisfied.
#
# In a later session we create a new dummy variable and build a
# logistic regression model.

# We can look at the distribution by months and see if there is any change.

rxHistogram(~arr_delay|month,
            airline2.xdf, 
            rowSelection=arr_delay>-30 & arr_delay<600)

## rxCrossTab & rxCube - Aggregate data

# What's the average Arrival Delay by Month and Carrier.

rxCrossTabs(arr_delay ~ unique_carrier:month, airline2.xdf, means=TRUE)
rxCrossTabs( ~ unique_carrier:month, airline2.xdf)

# What's the average Arrival Delay by month and Carrier.

rxCube(arr_delay~unique_carrier:month, airline2.xdf)
rxCube(~unique_carrier:month, airline2.xdf)

# NOTE: Please find out the difference between rxCrossTabs and rxCube

## rxCrossTabs/rxCube + R packages for visualization 

# Draw HeatMap of Average Arrival Delays by month and Carrier 

tmp1   <- rxCrossTabs(arr_delay ~ unique_carrier:month, airline2.xdf, means=TRUE)
result <- rxResultsDF(tmp1, output="means")

mySub  <- apply(result, 1, function(x) sum(is.na(x)))
result <- result[mySub != 12,]

pp     <- colnames(result)[-1]
pp     <- substring(pp,7)
colnames(result)[-1] <- pp

library(gplots)
par(oma=c(2,2,2,2))
heatmap.2(data.matrix(result[,-1]), 
          Colv=FALSE,
          labRow=result[,1],
          col=rev(heat.colors(16)),
          trace='none',
          symkey=FALSE,
          key.ylab="Mean")

# Draw a Line Plot of Average Arrival Delays by Carrier and DayOfWeek.

tmp2   <- rxCube(arr_delay~unique_carrier:day_of_week, airline2.xdf)
result <- rxResultsDF(tmp2)

result %>%
  ggplot(aes(x=day_of_week,
             y=arr_delay,
             group=unique_carrier,
             colour=unique_carrier)) + 
  geom_line(size=.75) + 
  geom_point() + 
  xlab("Weekday") + 
  ylab("Average of Arrival Delays") + 
  theme_minimal()

######################################
## Data Management - Lab - 10 min
######################################


## Connect to the 3.2 GB of Airline Data in XDF format 'AirOnTime87to12.xdf' (file.path)

## View data information (rxGetInfo)

## Summarize the following variables 
## Year, Month, DayOfWeek, UniqueCarrier, ArrDelay, ArrDel15, ArrDelayMinutes, Distance (rxSummary)

## Compute the quartiles of the Arrival Delay column in the data set (rxQuantile)

## Draw a histogram of ArrDelay (rxHistogram)

## Count the number of Arrival Delay by Carrier and draw a bar chart
## (rxCube & ggplot)

## Count the number of Arrival Delay by month and Carrier and then
## draw a line chart (rxCube & ggplot)

## Manipulate the dataset 
## Select the rows where ArrDelay larger than -30 and smaller than 600 and Year equal to 2012
## Keep the following variables: "Year", "Month", "DayofMonth", "DayOfWeek", 
##			                    "UniqueCarrier", "FlightNum", "Origin", "Dest", "Distance",
##                               "CRSDepTime", "CRSArrTime", "ArrDelay", "ArrDelayMinutes", "ArrDel15".
## Output the data as a new XDF file named as 'AirOnTime2012.xdf'.
## Check the dataset information again. (rxDataStep & blocksPerRead)


#################################################
## Predictive Modeling with PEMAs - Demo - 10 min
#################################################


## Before modeling
## Data filtering

airlineTrainTest.xdf <-
  file.path(output.path, paste0(fstem, "normTrainTest.xdf")) %T>%
  print()

rxDataStep(airline2.xdf,
           outFile=airlineTrainTest.xdf,
           rowSelection = !month %in% c('September', 'October',
                                        'November', 'December') & 
             arr_delay > -30 & arr_delay < 600 &
             origin %in% c('HNL', 'TPA', 'MEM', 'MIA', 'HOU', 'JFK', 'EWR',
                           'LAX', 'SFO', 'ORD', 'ATL', 'AUS'),
           transforms=list(isa_delay = ifelse(arr_delay>15,1,0),
                           urv=factor(ifelse(runif(length(year))<0.8,
                                             'TRAIN', 'TEST'))),
           overwrite=TRUE)

rxGetInfo(airlineTrainTest.xdf, TRUE, numRows=3)

## Split the dataset into training/testing datasets.

train.xdf <-
  file.path(output.path, paste0(fstem, 'normTrainTest.urv.TRAIN.xdf')) %T>%
  print()
test.xdf  <-
  file.path(output.path, paste0(fstem, 'normTrainTest.urv.TEST.xdf')) %T>%
  print()

rxSplit(airlineTrainTest.xdf,
        outFilesBase=airlineTrainTest.xdf,
        splitByFactor='urv',
        overwrite=TRUE)

######################
## Logistic Regression
#######################

## Build Logistic Regression model.

r1 <- rxLogit(isa_delay ~ month + day_of_week + unique_carrier,
              train.xdf,
              blocksPerRead=5)
summary(r1)

## Predict Logistic Model on our test dataset.

rxPredict(r1,
          data=test.xdf,
          computeResiduals=TRUE,
          predVarNames='LogitPredict',
          overwrite=TRUE)
rxGetInfo(test.xdf, getVarInfo=TRUE, numRows=10)

## Draw a ROC curve.

rxRocCurve(actualVarName='isa_delay', predVarNames='LogitPredict', data=test.xdf)

## Include Major Airports to see whether it helps to improve the model accuracy

r2 <- rxLogit(isa_delay ~ month + day_of_week + unique_carrier + origin, 
              train.xdf,
              blocksPerRead=5)

r2 %>% rxPredict(data=test.xdf,
                 computeResiduals=TRUE,
                 predVarNames='logit_with_origin',
                 overwrite=TRUE)

rxRocCurve(actualVarName='isa_delay',
           predVarNames='logit_with_origin',
           data=test.xdf)

##################
## Decision Tree
##################

## Build Decision Tree model. Have a look at htop to confirm all cores
## are being used.

d1 <- rxDTree(isa_delay ~ month + day_of_week + unique_carrier + origin,
              train.xdf,
              maxDepth=5,
              blocksPerRead=5)
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

d1 %>% rxPredict(data=test.xdf,
                 computeResiduals=TRUE,
                 predVarNames='dtree_with_origin',
                 overwrite=TRUE)

## Model evaluation

rxRocCurve(actualVarName='isa_delay',
           predVarNames=c('dtree_with_origin', 'logit_with_origin'),
           data=test.xdf)


###############################################
## Predictive Modeling - Lab - 10 min
###############################################

## Build a Glm model on the Airline Data in Year 2012 'AirOnTime2012.xdf‘ (rxGlm)
## using formula=ArrDelayMinutes~DayOfWeek + F(CRSDepTime) + Distance

## Build a Decision Forest model on the Airline Data in Year 2012 'AirOnTime2012.xdf‘ (rxDForest)
## using formula=ArrDel15 ~ Month + DayOfWeek + UniqueCarrier

## Build a Kmeans clustering model on the Airline Data in Year 2012 'AirOnTime2012.xdf‘ (rxKmeans)
## formula=~ ArrDelay + Distance

## Build a Logistic regression model on the Airline Data in Year 1987-2012 'AirOnTime87to12.xdf‘ (rxLogit)
## formula=ArrDel15 ~ Month + DayOfWeek + UniqueCarrier

## ?rxDForest

###############################################
## Model Deployment - Demo - 20 min
###############################################

## Deploy R models in SQL Server 2016
# See the demonstration
# Try it out at home
# 1. Set up a SQL 2016 Server virtual machine using your 200 USD free azure subscription.
# 2. Execute R script 'AirlineInSql.R' from Rstudio.
# 3. Execute sql scripts 'airlineDemo.sql' and then 'airlineStepByStep.sql' from SSMS.

## Deploy R models in Hadoop environment
# We will deep dive into MRS in Hadoop in the next session. 
# Have a break and relax!
