/*
Expectations/Assumptions: 
The query is written with the understanding of the use case, based on the exact use case the code may need further chnages.
As we dont have the exact data and the datawarehouse I have written the query using only the table names, not having the database/projectId, schema/dataset details.
I have added the realted metrics like in addition to the creative names, which may be useful for the downstream teams for their analysis or to display in the dashboards.
Finally, I have crated a view rather than a query, so that it would be helpful for the downstream team to fetch the data from the view and also we can materialize the view if we need to improve the perfromance of the data retreival
*/


create or replace view vw_ad_metrics as

/* Extarcting the video records with video_bp_count > 9 */
with video_above9 as
(select media_plan_id
      , creative_name
      , above9 = sum(CASE WHEN coalesce(video_bp_count,0) >= 9 THEN 1 ELSE 0 END)
      , creative_name_count = count(creative_name)
 FROM video_best_practices_data
    group by media_plan_id, creative_name, video_bp_count),

/* Summarising the video best practice records and calculating the percentage of the creative names with video_bp_count >= 9 */
with video_above9_summary as
(select media_plan_id
      , creative_name as vid_creative_name
      , vid_creatives_above9_cnt = sum(above8)
      , vid_creatives_cnt = sum(creative_name_count)
      , vid_creatives_above9_pct = sum(above9) / sum(creative_name_count) * 100
 from video_above9
group by media_plan_id, creative_name),

/* Extarcting the banner records with banner_bp_count > 8 */
with banner_above8 as
(select media_plan_id
      , creative_name
      , above8 = sum(CASE WHEN coalesce(banner_bp_count,0) >= 8 THEN 1 ELSE 0 END)
      , creative_name_count = count(creative_name)
 FROM banner_best_practices_data
    group by media_plan_id, creative_name, banner_bp_count),

/* Summarising the banner best practice records and calculating the percentage of the creative names with banner_bp_count >= 8 */
with banner_above8_summary as
(select media_plan_id
      , creative_name as ban_creative_name
      , ban_creatives_above8_cnt = sum(above8)
      , ban_crreatives_cnt = sum(creative_name_count)
      , ban_creatives_above8_pct = sum(above) / sum(creative_name_count) * 100
 from banner_above8
group by media_plan_id, creative_name),

/* Extarcting the creative testing records, those achived prinmary goal */
with success_goal as
(select creative_name
      , goal_achieved = sum(CASE WHEN coalesce(pri_passed,'NO') == 'YES' THEN 1 ELSE 0 END)
      , creative_name_count = count(creative_name)
 FROM creative_testing_tracker_data
    group by creative_name, pri_passed),

/* Summarising the creative testing records and calculating primary gaol achieved percentage */
with success_goal_summary as
(select creative_name as goal_creative_name
      , goal_achieved_cnt = sum(goal_achieved)
      , goal_creatives_cnt = sum(creative_name_count)
      , goals_achieved_pct = sum(goal_achieved) / sum(creative_name_count) * 100
 from success_goal
group by creative_name
)

/* Final query to hold the required information along with some additional information */
select rd.Campaign_Name
    , rd.Media_Plan_ID
    , rd.Product
    , cpil = case when abs_lift = 0 then -1
                  else spends / (reach * abs_lift)
             end
    , vd.vid_creative_name
    , vd.vid_creatives_above9_cnt
    , vd.vid_creatives_cnt
    , vd.vid_creatives_above9_pct
    , bn.ban_creative_name
    , bn.ban_creatives_above8_cnt
    , bn.ban_creatives_cnt
    , bn.ban_creatives_above8_pct
    , gl.goal_creative_name
    , gl.goal_achieved_cnt
    , gl.goal_creatives_cnt
    , gl.goals_achieved_pct
from results_data rd
left join video_above9_summary vd on rd.media_plan_id = vd.media_plan_id
left join banner_above8_summary bn on rd.media_plan_id = bn.media_plan_id
left join success_goal_summary gl on (gl.goal_creative_name = vd.vid_creative_name or gl.goal_creative_name or bn.goal_creative_name)