setwd("C:/Users/Prasanta/Downloads/PGPBABI/Capstone")
PlumbData <- read.csv("Target Marketing and cross selling - Data.csv")
View(PlumbData)
PlumbDataOrg <- PlumbData

library(dplyr)
library(sqldf)
library(car)

str(PlumbData)

PlumbData$Setup.Date <- as.POSIXct(PlumbData$Setup.Date,format = "%d-%m-%Y")
PlumbData$Last.Service.Date <- as.POSIXct(PlumbData$Last.Service.Date,format = "%d-%m-%Y")
PlumbData$Call.Date <- as.POSIXct(PlumbData$Call.Date,format = "%d-%m-%Y")
PlumbData$Complete.Date <- as.POSIXct(PlumbData$Complete.Date,format = "%d-%m-%Y")
PlumbData$Week.Ending.Date <- as.POSIXct(PlumbData$Week.Ending.Date,format = "%d-%m-%Y")
PlumbData$Schedule.Date <- as.POSIXct(PlumbData$Schedule.Date,format = "%d-%m-%Y")
PlumbData$Dispatch.Date <- as.POSIXct(PlumbData$Dispatch.Date,format = "%d-%m-%Y")
View(PlumbData)
str(PlumbData)

## Converting Customer.ID from Factor to Character type and trimming
PlumbData$Customer.ID <- as.character(PlumbData$Customer.ID)
PlumbData$Customer.ID <- trimws(PlumbData$Customer.ID)

str(PlumbData)
## PlumbData <- PlumbData
PlumbData$Sunday <- 0
PlumbData$Monday <- 0
PlumbData$Tuesday <- 0
PlumbData$Wednesday <- 0
PlumbData$Thursday <- 0
PlumbData$Friday <- 0
PlumbData$Saturday <- 0

PlumbData$Sunday <- ifelse(as.POSIXlt(PlumbData$Call.Date)$wday == 0,1,0)
PlumbData$Monday <- ifelse(as.POSIXlt(PlumbData$Call.Date)$wday == 1,1,0)
PlumbData$Tuesday <- ifelse(as.POSIXlt(PlumbData$Call.Date)$wday == 2,1,0)
PlumbData$Wednesday <- ifelse(as.POSIXlt(PlumbData$Call.Date)$wday == 3,1,0)
PlumbData$Thursday <- ifelse(as.POSIXlt(PlumbData$Call.Date)$wday == 4,1,0)
PlumbData$Friday <- ifelse(as.POSIXlt(PlumbData$Call.Date)$wday == 5,1,0)
PlumbData$Saturday <- ifelse(as.POSIXlt(PlumbData$Call.Date)$wday == 6,1,0)
## Converting time data from int to time format and then finding out the differences in time

#PlumbData$Complete.Time <- as.character(PlumbData$Complete.Time)
PlumbData$Complete.Time <- sprintf("%04d", PlumbData$Complete.Time)
PlumbData$Complete.Time <- format(strptime(PlumbData$Complete.Time, format="%H%M"), format = "%H:%M")
#PlumbData$Complete.Time <- as.character(PlumbData$Complete.Time)
#PlumbData$Complete.Time <- as.POSIXct(PlumbData$Complete.Time, format = "%H:%M")
#PlumbData$Complete.Time <- strptime(PlumbData$Complete.Time, format = "%H:%M")

#PlumbData$Dispatch.Time <- as.character(PlumbData$Dispatch.Time)
PlumbData$Dispatch.Time <- sprintf("%04d", PlumbData$Dispatch.Time)
PlumbData$Dispatch.Time <- format(strptime(PlumbData$Dispatch.Time, format="%H%M"), format = "%H:%M")

#PlumbData$Schedule.Time <- as.character(PlumbData$Schedule.Time)
PlumbData$Schedule.Time <- sprintf("%04d", PlumbData$Schedule.Time)
PlumbData$Schedule.Time <- format(strptime(PlumbData$Schedule.Time, format="%H%M"), format = "%H:%M")

#PlumbData$Call.Time <- as.character(PlumbData$Call.Time)
PlumbData$Call.Time <- sprintf("%04d", PlumbData$Call.Time)
PlumbData$Call.Time <- format(strptime(PlumbData$Call.Time, format="%H%M"), format = "%H:%M")

PlumbData$Complete.DateTime <- as.POSIXct(paste(PlumbData$Complete.Date, PlumbData$Complete.Time), format="%Y-%m-%d %H:%M")
PlumbData$Dispatch.DateTime <- as.POSIXct(paste(PlumbData$Dispatch.Date, PlumbData$Dispatch.Time), format="%Y-%m-%d %H:%M")
PlumbData$Schedule.DateTime <- as.POSIXct(paste(PlumbData$Schedule.Date, PlumbData$Schedule.Time), format="%Y-%m-%d %H:%M")
PlumbData$Call.DateTime <- as.POSIXct(paste(PlumbData$Call.Date, PlumbData$Call.Time), format="%Y-%m-%d %H:%M")


#PlumbData$ActualTimeToComplete <- PlumbData$Complete.DateTime - ifelse(PlumbData$Dispatch.DateTime > PlumbData$Complete.DateTime, PlumbData$Call.DateTime,PlumbData$Dispatch.DateTime)
#PlumbData$TotalTimeToComplete <- PlumbData$Complete.DateTime - ifelse(PlumbData$Schedule.DateTime > PlumbData$Complete.DateTime, PlumbData$Call.DateTime, PlumbData$Schedule.DateTime)

PlumbData$ActualTimeToComplete <- PlumbData$Complete.DateTime - PlumbData$Dispatch.DateTime
PlumbData$DelayInResponseTime <- ifelse((PlumbData$Dispatch.DateTime > PlumbData$Schedule.DateTime),(PlumbData$Dispatch.DateTime - PlumbData$Schedule.DateTime),0)
#PlumbData$TotalTimeToComplete <- PlumbData$Complete.DateTime - PlumbData$Schedule.DateTime

## Preparing summary Data

PlumbData <- PlumbData %>% arrange(Call.DateTime) %>%
  group_by(Customer.ID,Customer.Type,Address,City,State) %>%
  mutate(DiffCallDateTime = Call.DateTime - lag(Call.DateTime, default=first(Call.DateTime)))

PlumbDataSumm <- PlumbData %>% 
  group_by(Customer.ID,Customer.Type,Address,City,State) %>%                            # multiple group columns
  summarise(SUMActualTimeToCompleteInHrs = sum(ActualTimeToComplete)/3600, 
            SUMDelayInResponseTimeInHrs = sum(DelayInResponseTime)/3600, SUMTicketRevenue = sum(Ticket.Revenue), 
            CustomerRecentVisit = max(Complete.DateTime), SumDiffInConsecutiveCallsInHrs = sum(DiffCallDateTime)/3600,
            DistinctJobCodeCounts = n_distinct(Job.Code), DistinctRevCodeCounts = n_distinct(Rev.Code),
            TotCallsOnSunday = sum(Sunday),TotCallsOnMonday = sum(Monday),TotCallsOnTuesday = sum(Tuesday),
            TotCallsOnWednesday = sum(Wednesday),TotCallsOnThursday = sum(Thursday),TotCallsOnFriday = sum(Friday),
            TotCallsOnSaturday = sum(Saturday),CustomerFrequency = n())

#PlumbDataSumm$SUMTotalTimeToComplete <- as.numeric(PlumbDataSumm$SUMTotalTimeToComplete)

PlumbDataSumm$SUMActualTimeToCompleteInHrs <- as.numeric(PlumbDataSumm$SUMActualTimeToCompleteInHrs)

PlumbDataSumm$AvgActualTimeToCompleteInHrs <- (PlumbDataSumm$SUMActualTimeToCompleteInHrs/PlumbDataSumm$CustomerFrequency)
PlumbDataSumm$AvgDiffInConsecutiveCallsInHrs <- ifelse((PlumbDataSumm$CustomerFrequency-1) == 0,0,(PlumbDataSumm$SumDiffInConsecutiveCallsInHrs/(PlumbDataSumm$CustomerFrequency-1)))
PlumbDataSumm$AvgDelayInResponseTimeInHrs <- (PlumbDataSumm$SUMDelayInResponseTimeInHrs/PlumbDataSumm$CustomerFrequency)


#PlumbDataSumm$AvgTotalTimeToCompleteInHrs <- (PlumbDataSumm$SUMTotalTimeToComplete/PlumbDataSumm$CustomerFrequency)/3600
#PlumbDataSumm$AvgDiffInConsecutiveCallsInHrs <- ifelse((PlumbDataSumm$CustomerFrequency-1) == 0,0,(PlumbDataSumm$SumDiffInConsecutiveCalls/(PlumbDataSumm$CustomerFrequency-1))/3600)
View(PlumbDataSumm)

# str(PlumbData2)
# 
# PlumbData2$Customer.ID <- as.character(PlumbData2$Customer.ID)
# 
# 
# PlumbDataSumm <- NULL
# PlumbData <- NULL
# 
# 
# Data2013 <- NULL
# Data2013 <- PlumbData2 %>% distinct(Customer.ID)
# 
# View(Data2013)
# str(Data2013)
# 
# PlumbData2Final <- merge(Data2013, PlumbDataSumm, by = "Customer.ID")
# View(PlumbData2Final)

write.csv(PlumbDataSumm,"Cleaned.csv")