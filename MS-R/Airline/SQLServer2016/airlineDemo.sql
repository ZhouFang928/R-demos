------------------------------------------------------------------------------------------
--Title: Airline Demo (Execute R  via T-SQL)
--Author: IMML Algorithm & Data Science, Microsoft
------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------
--Choose database to use
------------------------------------------------------------------------------------------
use RevoTestDB
go

------------------------------------------------------------------------------------------
--View tables
------------------------------------------------------------------------------------------
select top 10 * from dbo.AirlineDemoSmall;
go

--------------------------------------------------------------------------------------------
--Create stored procedures to train models
--------------------------------------------------------------------------------------------
--Create a table to store modeling results
drop table if exists airline_rx_models;
go
create table airline_rx_models(
	model_name varchar(30) not null default('default model') primary key,
	model varbinary(max) not null
);
go

--Create a stored procedure to train linear regression model with RevoScaleR 
drop procedure if exists generate_airline_rx_linear;
go
create procedure generate_airline_rx_linear
as
begin
	execute sp_execute_external_script
	  @language = N'R'
	, @script = N'
		require("RevoScaleR");
        linear_model<-rxLinMod(formula=ArrDelay~CRSDepTime+DayOfWeek, data=AirlineDemoSmall)
		print(linear_model)
		rxLinMod_model <- data.frame(payload = as.raw(serialize(linear_model, connection=NULL)));
       '
	, @input_data_1 = N'select * from AirlineDemoSmall'
	, @input_data_1_name = N'AirlineDemoSmall'
	, @output_data_1_name = N'rxLinMod_model'
	with result sets ((model varbinary(max)));
end;
go

--Update rxLinMod modeling results
insert into airline_rx_models (model)
exec generate_airline_rx_linear;
update airline_rx_models set model_name = 'rxLinMod' where model_name = 'default model';
select * from airline_rx_models;
go

--------------------------------------------------------------------------------------------
--Create stored procedures to score models
--------------------------------------------------------------------------------------------
--Create a stored procedure to score linear regression Model with RevoScaleR
drop procedure if exists predict_airline_rx_linear;
go
create procedure predict_airline_rx_linear (@model varchar(100))
as
begin
	declare @rx_model varbinary(max) = (select model from airline_rx_models where model_name = @model);
	-- Predict based on the specified model:
	exec sp_execute_external_script 
					@language = N'R'
				  , @script = N'
                     require("RevoScaleR");
                     linear_model<-unserialize(rx_model);
                     airlinepred <- rxPredict(modelObject = linear_model,
                                              data = AirlineDemoSmall,
                                              overwrite = TRUE)
                     OutputDataSet <- cbind(AirlineDemoSmall[c(4,3,2,1)], airlinepred$ArrDelay_Pred);
                     colnames(OutputDataSet) <- c("FlightNum", "DayOfWeek", "CRSDepTime", "ArrDelayActual", "ArrDelayExpected");
                     print(OutputDataSet)
                    '
	, @input_data_1 = N'select top 10 * from AirlineDemoSmall'
	, @input_data_1_name = N'AirlineDemoSmall'
	, @output_data_1_name=N'AirlineDemoSmallPred'
	, @params = N'@rx_model varbinary(max)'
	, @rx_model = @rx_model
	with result sets ( ("FlightNum" int, "DayOfWeek" bigint, "CRSDepTime" float, "ArrDelayActual" int, "ArrDelayExpected" float));
end;
go

--Execute scoring procedure
drop table if exists AirlineDemoSmallPred;
go
create table AirlineDemoSmallPred(
FlightNum int,
DayOfWeek int,
CRSDepTime float,
ArrDelayActual int, 
ArrDelayExpected float
);
go

insert into AirlineDemoSmallPred
exec predict_airline_rx_linear 'rxLinMod'
go

select top 10 * from AirlineDemoSmallPred