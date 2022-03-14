/*
This test is for the data provided as part of the task
*/
select * from vw_budget_pace
where [PLATFORM] = 'facebook'
and DATEPART(year,date_of_activity) = 2021
and DATEPART(month, date_of_activity) = 1
and region = 'emea'