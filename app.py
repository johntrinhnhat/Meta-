import json
import os
import gspread
import pandas as pd
import time
import aiohttp
import asyncio
from colorama import Fore, init
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
from datetime import datetime
from gspread_dataframe import set_with_dataframe

#Initialize variables
init(autoreset=True)
load_dotenv()

start_time = time.time()

async def getDateRange():
    current_month = datetime.now().month
    start_date = datetime(2024, current_month, 1)
    end_date = datetime.now()
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    return json.dumps({'since': start_date_str, 'until': end_date_str})

def authorizeGoogleSheets():
    """Authorize Google Sheets API and return the client."""
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    client = gspread.authorize(creds)
    return client

def extractAction(action_list, action_type):
    for action in action_list:
        if action['action_type'] == action_type:
            return action['value']
    return 0

async def fetchMeta(session, access_token, account_id):
    data = []
    time_range = await getDateRange()

    url = f'https://graph.facebook.com/v20.0/act_{account_id}/insights'
    params = {
        'access_token': access_token,
        'time_range': time_range,
        'fields': 'account_currency, account_name, campaign_name, adset_name, ad_name, impressions, clicks, spend, reach, actions, objective, outbound_clicks, video_thruplay_watched_actions, conversions',
        'time_increment': 1,
        'level': 'ad'
    }

    while url:
        async with session.get(url, params=params) as response:
            if response.status == 200:
                response_json = await response.json()
                data.extend(response_json['data'])
                url = response_json.get('paging', {}).get('next')
            else:
                print(f"Error: {response.status}")
                print(await response.text())
                break

    df = pd.DataFrame(data)

    required_columns = ['date_start', 'account_name', 'account_currency', 'campaign_name', 'adset_name', 'ad_name', 'impressions', 'clicks', 'spend', 'reach', 'actions', 'objective', 'outbound_clicks', 'video_thruplay_watched_actions', 'conversions']
    for column in required_columns:
        if column not in df.columns:
            df[column] = None

    action_columns = ['link_clicks', '3sec_plays', 'engagement', 'reaction', 'comments', 'lead', 'share', 'mess_conversation']
    action_types = ['link_click', 'video_view', 'post_engagement', 'post_reaction', 'comment', 'lead', 'post', 'onsite_conversion.messaging_conversation_started_7d']
    for action_col, action_type in zip(action_columns, action_types):
        df[action_col] = df['actions'].apply(lambda x: extractAction(x, action_type) if isinstance(x, list) else 0)
    df['outbound_clicks'] = df['outbound_clicks'].apply(lambda x: extractAction(x, 'outbound_click') if isinstance(x, list) else 0)
    df['thruPlays'] = df['video_thruplay_watched_actions'].apply(lambda x: extractAction(x, 'video_view') if isinstance(x, list) else 0)


    df = df.rename(columns={'date_start': 'date', 'account_currency': 'currency'})
    df = df.sort_values(by='date', ascending=False)
    df = df[['date', 'account_name', 'currency', 'campaign_name', 'adset_name', 'ad_name', 'spend', 'mess_conversation', 'lead', 'impressions', 'clicks', 'reach', 'link_clicks', '3sec_plays', 'thruPlays', 'engagement', 'reaction', 'comments', 'share','outbound_clicks', 'objective']]
    
    return df

async def googleSheetImport(fb_ads_data, sheet_name):
    """Imports the DataFrame to Google Sheets."""
    client = authorizeGoogleSheets()

    if sheet_name == 'Pur Artistry Brow & Lash Studio':
        sheet_name = 'PA'
    elif sheet_name == 'Purluxe Beauty Bar':
        sheet_name = 'PL'
    elif sheet_name == 'Club Well':
        sheet_name = 'CW'
    elif sheet_name == 'Shopify':
        sheet_name = 'Mimi'
    elif sheet_name == 'Eira Medical':
        sheet_name = 'Eira'
    else:
        sheet_name = 'GMA'

    sheet = client.open(f"{sheet_name} Ads (Auto)")
    try:
        worksheet = sheet.worksheet(f"Meta (Raw)")
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sheet.add_worksheet(title=sheet_name, rows=2000, cols=20)
    
    try:
        set_with_dataframe(worksheet, fb_ads_data, include_column_header=True, resize=True)
        print(f"\nImported data to Google Sheets ✅\n")
    except gspread.exceptions.APIError as e:
        print(f"Failed to update Google Sheets due to an API error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

async def processData(session, access_token, account_id):
    """Processes data for a given account ID."""
    df = await fetchMeta(session, access_token, account_id)

    if df.empty or 'account_name' not in df.columns or df['account_name'].isna().all():
        print(f"No data found for account ID: {account_id}")
        return
    
    print(f"{Fore.LIGHTRED_EX}{df['account_name'][0]}:")

    data = df.drop(columns=['account_name'])
    print(data, data.columns)
    
    sheet_name = df['account_name'][0].strip()
    await googleSheetImport(data, sheet_name)

async def main():
    access_token = os.getenv("fb_access_token")
    account_ids = [
        os.getenv('account_id_2'),
        os.getenv('account_id_3'),
        os.getenv('account_id_4'),
        os.getenv('account_id_1'),
        os.getenv('account_id_5'),
        os.getenv('account_id_6')
        ]

    async with aiohttp.ClientSession() as session:
        tasks = [processData(session, access_token, account_id) for account_id in account_ids]
        await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(main())
