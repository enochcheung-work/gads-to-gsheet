from google.oauth2 import service_account
import pandas_gbq
import gspread
import gspread_pandas
from gspread_pandas import Spread, Client
import numpy as np
import pandas as pd


credentials = service_account.Credentials.from_service_account_file('./google_secret.json')

sql_campaign_cost = """
Select a.Cost, a.Date, c.CampaignName 
FROM
    (Select sum(Cost)/1000000 as Cost, Format_Date("%F", Date) as Date, CampaignId
    FROM `{your_project}.google_ads.AdStats_{your_id}`
    GROUP BY Date, CampaignId) a
    LEFT JOIN(
    Select CampaignName, CampaignId
    FROM `{your_project}.google_ads.Campaign_{your_id}`
    GROUP BY CampaignName, CampaignId)c
    ON c.CampaignId = a.CampaignId
    WHERE a.Cost > 0"""

sql_video_views = """
SELECT 
c.CampaignName, 
c.CampaignId,
c.CampaignStatus,
SUM(vbs.VideoViews) as VideoViews, 

FROM `{your_project}.google_ads.AgeRangeNonClickStats_{your_id}` vbs

left join
    (select CampaignName, CampaignId, CampaignStatus
    from `{your_project}.google_ads.Campaign_{your_id}`
    group by CampaignName, CampaignId, CampaignStatus) c
on vbs.CampaignId = c.CampaignId

where c.CampaignStatus = "ENABLED"
group by c.CampaignName, c.CampaignID, c.CampaignStatus"""


specific_campaign_cost = """
select a.Date, (a.Cost / 1000000) as Cost,a.Impressions, a.Clicks, a.CampaignId,b.CampaignName from
(SELECT date, CampaignId, sum(cost) as cost, sum(Impressions) as Impressions, sum(Clicks) as Clicks
FROM `{your_project}.google_ads.CampaignBasicStats_{your_id}` 
where CampaignId = {your_campaign_id}
group by date, CampaignId
)a
left join 
(select distinct CampaignName, CampaignId
from `{your_project}.google_ads.Campaign_{your_id}` 
where regexp_contains(CampaignName, "{your_campaign_name"))b
on a.CampaignId = b.CampaignId
order by a.date DESC """




scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

sql_pair = {sql_campaign_cost:"Sheet1", sql_video_views:"Sheet2", specific_campaign_cost:"Sheet3"} 

class BQtoGS:

    def bq2gs(sql,sheet):
        try:
            df = pandas_gbq.read_gbq(sql, project_id="{your_project_id}", credentials=credentials)
            c = gspread_pandas.conf.get_config(conf_dir='./', file_name='google_secret.json')
            print('config get')
            spread = Spread('{your_gsheet}', config=c)
            print('spreaded')
            spread.df_to_sheet(df, index=False, sheet=sheet, start='A1', replace=True)
            print("done - " + sheet)
        except:
            "spread not done"
    
def main(self):
    for x,y in sql_pair.items():
        BQtoGS.bq2gs(x,y)
    return {}
    
if __name__ == '__main__':
    main(None)
