------------------------------------------------------------------------------------------
--Title: Airline Demo (Step by Step)
--Author: IMML Algorithm & Data Science, Microsoft
------------------------------------------------------------------------------------------

-- Step 1 - View airline data
select count(*) from dbo.AirlineDemoSmall
select top 10 * from dbo.AirlineDemoSmall
go

-- Step 2 - Train the airline model
-- After successful execution, this will create a binary representation of the model

exec generate_airline_rx_linear;

-- In this workshop, we have already stored the model in the table airline_rx_models
select * from airline_rx_models
where model_name = 'rxLinMod'


-- Step 3 (Predict) - In this step, you wiull invoke the stored procedure predict_airline_linear
-- The stored procedure uses the rxPredict function to predict the arrival delay
-- Results are returned as an output dataset
exec predict_airline_rx_linear 'rxLinMod';

