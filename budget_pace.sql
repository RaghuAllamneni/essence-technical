 /*
Expectations/Assumptions: 
It is expected to have the platform names to be consistent across the data sets.
If not, it is expected to have the mapping details between different names/versions of the same platform in a lookup table
For now, in the below query the platform names in the media_budget_data is explicitly converted to match the same we are getting in the market_spend_data

I have created a view, instead of a straight query, so that it will be easier to call by the downstream teams/processes.
Also this will help us converting it into a materialised view if we need to increase the perfornace.
Also it will give flexibility to have some additional columns which can be useful by other downstream teams/processes (ofcourse, depending on the security levels)

 */
 create or replace view vw_budget_pace as

 /* unpivoting the budget allotted for different platforms */
 with Monthly_Budget as
 (select [Year]
		, [Month]
		, [platform] = Case When [platform] = 'DV360' Then 'display video 360'
							When [platform] = 'FB' Then 'facebook'
							When [platform] = 'G-Ads' Then 'google-ads'
						End
		, [Channel] = Case when coalesce([display_budget], 0 ) > 0 then 'display'
						   when coalesce([search_budget], 0 ) > 0 then 'search'
						   when coalesce([social_budget], 0 ) > 0 then 'social'
					  End
		, [region]
		, coalesce([display_budget], 0 ) + coalesce([search_budget], 0 ) + coalesce([social_budget], 0 ) as [budget]
 From [Essence].[dbo].[Media_Budget_Data]),

 /* CTE to hold the market spend data inclusing the daily cumulative spend */
 Market_Spend AS (select msd.[date_of_activity]
      ,msd.[channel]
      ,msd.[platform]
      ,msd.[region]
      ,msd.[market_spend]
	  ,cum_sum_spend = sum(msd.[market_spend]) over(partition by datepart(year,msd.[date_of_activity]), datepart(Month,msd.[date_of_activity])
      ,msd.[channel]
      ,msd.[platform]
      ,msd.[region]
	  order by msd.[date_of_activity])
FROM [Essence].[dbo].[Market_Spend_Data] msd)

/* Final query to calculate the budget pacing as well as time pacing */
select ms.[date_of_activity]
      ,ms.[channel]
      ,ms.[platform]
      ,ms.[region]
      ,ms.[market_spend]
	  ,cum_sum_spend
	  ,budget_pacing = FORMAT(cum_sum_spend / mb.budget, 'P2')
	  ,days_elapsed = datepart(day, ms.[date_of_activity])
	  ,time_pacing = FORMAT(cast(datepart(DAY, ms.[date_of_activity]) as float) / DAY(eomonth(ms.[date_of_activity])), 'P2')
FROM Market_Spend ms
join Monthly_Budget mb
on 1 = 1
and datepart(year,ms.[date_of_activity]) = mb.[Year]
and LEFT(Lower(datename(MONTH,ms.[date_of_activity])),3) = LOWER(mb.[Month])
and ms.platform = mb.platform
and ms.Channel = mb.Channel
and ms.region = mb.region