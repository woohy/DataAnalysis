import datetime
import pandas as pd
import numpy as np
from pandas import DataFrame
from pandasql import *
import phoenixdb
import phoenixdb.cursor
from pyhive import hive
from google.cloud import bigquery
from google.oauth2.service_account import Credentials

# 유니크 카테고리 가져오기
unique_key = pd.read_csv('file_name',
                         encoding='euc-kr',
                         dtype={'depth1_num_code': str, 'depth2_num_code': str, 'depth3_num_code': str,
                                'depth4_num_code': str, 'code': str})

###################################################
#
# Hive 연동
#
###################################################
conn = hive.Connection(host='-',
                       port=0,
                       username='-')

def query_HQL(query):
    query_result = pd.read_sql(query, conn)
    return pd.DataFrame(query_result)

    conn.close()

###################################################
#
# Phoenix 연동
#
###################################################
def query_phenixdb(query):
    database_url = '-'
    conn = phoenixdb.connect(database_url, autocommit=True)
    cursor = conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)

    cursor.execute(query)
    result = cursor.fetchall()
    return pd.DataFrame(result)
    conn.close()


##################################################################
#
# 서브도메인 데이터 추출 -> FINAL_SUBSERVICE
#
##################################################################
query1 = """
select UIDA, category4 as category, sum(value) as value, 'Sub service' as type
from DB.Table
where dt_weekly
AND (UIDA is not null and UIDA != '')
group by UIDA, category4
"""

# Final query
query1_dt = query1.replace('dt_weekly', dt)

##################################
# FINAL_subService 최종 데이터
##################################
FINAL_subService = query_HQL(query1_dt)

# 카테고리 통합안 반영
# 대외비 삭제

##################################################################
#
# DA 광고 데이터 추출
#
##################################################################

#################################################
#
# [Table1] : campaign_category
# 빅쿼리 내 DA 광고 카테고리 정보 가져오기 (GCP DATA)
#
#################################################
SCOPES = [
    '',
    '',
    ''
]

credentials = Credentials.from_service_account_file(
    'file_name')
credentials = credentials.with_scopes(SCOPES)
client = bigquery.Client(project='project_name', credentials=credentials)
query = "select * from DB.Table"
df_result = client.query(query).to_dataframe()


#################################################
# 캠페인 카테고리 추출 (campaign_name, business_cate_large)
#################################################
campaign_category = DA_LIST[['campaign_name', 'business_cate_large']].drop_duplicates()
campaign_category['campaign_name'] = campaign_category['campaign_name'].str.replace(' ', '', regex=True)

#################################################
#
# [Table2] : click_df
# DA 광고 클릭 데이터 불러오기(zuid, prop_adid)
#
#################################################
conn = hive.Connection(host='-',
                       port=0,
                       username='-')

def query_HQL(query):
    query_result = pd.read_sql(query, conn)
    conn.close()

    return pd.DataFrame(query_result)


query2 = """
select uida, prop_adid
from DB.Table
where dt_weekly
and event = 'event_name'
and prop_adid > ''
and prop_adid is not null
and prop_pos in ('bn_lefttop_big', 'bn_brwidget_big')
and uida is not null
and regexp_replace(uida, '[^ᅡ-ㅣa-zA-Z0-9ᄀ-ᄒ가-힣]', '') != ''
"""

# Final query
query2_dt = query2.replace('dt_weekly', dt)
print(query2_dt)

##################################
# FINAL_subService 최종 데이터
##################################
click_df = query_HQL(query2_dt)
click_df = click_df.fillna('')
click_df['prop_adid'] = click_df['prop_adid'].astype(int)


#################################################
#
# [Table3] netinsight_df
# 넷인사이트 광고주 정보 가져오기(ads_id, 광고업종명)
#
#################################################
# phoenixdb
def query_phenixdb(query):
    database_url = '-'
    conn = phoenixdb.connect(database_url, autocommit=True)
    cursor = conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)

    cursor.execute(query)
    result = cursor.fetchall()
    return pd.DataFrame(result)
    conn.close()


netinsight_df = query_phenixdb("select * from DB.Table")

# 배너 & 브랜딩위젯 필터링
netinsight_df = netinsight_df[
    (netinsight_df['ADS_NAME'].str.contains('banner_name')) | (netinsight_df['ADS_NAME'].str.contains('widget_name'))]

# 캠페인명 추출
netinsight_df['re_campaign'] = netinsight_df['ADS_NAME'].str.split('_', expand=True)[2]
netinsight_df = netinsight_df[['ADS_ID', 'COMPANY_NAME', 're_campaign']]

##################################################################
#
# Join Data
#
##################################################################
# [join1] 사용자 클릭 로그 + 광고id별 업종명(netinsight_df)
df = click_df.merge(netinsight_df[['ADS_ID', 're_campaign']], left_on='prop_adid', right_on='ADS_ID', how='left')[
    ['uida', 'ADS_ID', 're_campaign']]

# [join2] join1 + DA GCP Data
data = df.merge(campaign_category[['campaign_name', 'business_cate_large']], left_on='re_campaign',
                right_on='campaign_name', how='left')

##################################################################
#
# Final_DA_DATA
#
##################################################################
data1 = data[['uida', 'ADS_ID', 'campaign_name', 'business_cate_large']]


##################################
# FINAL_DA 최종 데이터
##################################
da_data = data1.value_counts(dropna=False).groupby(['uida', 'business_cate_large']).agg(value='count').reset_index()
da_data = da_data[da_data['business_cate_large'].notnull()]

# 카테고리에 depth4값으로 변환
da_data = da_data.merge(unique_key, left_on='business_cate_large', right_on='depth4', how='left')[
    ['uida', 'depth4', 'value']]
# rename column
da_data.rename(columns={'depth4': 'category'}, inplace=True)
FINAL_DA = da_data[da_data['category'].notnull()]

FINAL_DA['type'] = 'Display AD'

##################################################################
#
# MKTS 데이터 추출 -> shopping_mkts
#
##################################################################

temp0 = DataFrame()

data = ['MKTS_BEST_LOGS', 'MKTS_BEST_PROMOTION_LOGS', 'MKTS_BEST_TEXTLINK_LOGS', 'MKTS_PREMIUM_LOGS', 'MKTS_SB_LOGS']
for i in range(0, 5):
    query = "select UIDA, ACCOUNTS_CATEGORY_1, ACCOUNTS_CATEGORY_2, ACCOUNTS_CATEGORY_3, ' " + data[
        i] + "' as groups from " + data[i] + " Where dt_weekly"

    pheonix_query = query.replace('dt_weekly', dt).replace('-', '')

    print(pheonix_query)
    temp = query_phenixdb(pheonix_query)

    temp0 = pd.concat([temp0, temp])

shopping = temp0[['UIDA', 'ACCOUNTS_CATEGORY_2', 'ACCOUNTS_CATEGORY_3']]


##################################
# UIDA별 카테고리별 데이터 집계
##################################
SHOPPING_MKTS = shopping.value_counts(dropna=False).groupby(['UIDA', 'category']).agg(value='count').reset_index()
SHOPPING_MKTS.rename(columns={'UIDA': 'uida'}, inplace=True)
FINAL_ShoppingMKTS = SHOPPING_MKTS[SHOPPING_MKTS['category'].notnull()]

FINAL_ShoppingMKTS['type'] = 'Shopping MKTS'

####################################################################
# 최종 데이터
####################################################################
total = pd.concat([FINAL_ShoppingMKTS, FINAL_DA, FINAL_subService])

# type 분리를 위한 카테고리 병합
total['category_re'] = total['category'] + "&" + total['type']
total = total.groupby(['uida', 'category_re'])['value'].agg(ck='sum').reset_index()

# data long to wide
total_w = total.reset_index().pivot(index='uida', columns='category_re', values='ck')

# NaN to 0
total_w = total_w.fillna(0)

#####################################################
# 유저 관심사 추정 (Scoring)
#####################################################
# 대외비 삭제

# Quantile 부여
final_data_long_not0["quantile"] = np.where(
    (final_data_long_not0['scoring'] >= 0) & (final_data_long_not0['scoring'] < 20), 5,
    np.where((final_data_long_not0['scoring'] >= 20) & (final_data_long_not0['scoring'] < 40), 4,
             np.where((final_data_long_not0['scoring'] >= 40) & (final_data_long_not0['scoring'] < 60), 3,
                      np.where((final_data_long_not0['scoring'] >= 60) & (final_data_long_not0['scoring'] < 80), 2,
                               1))))

# 카테고리 통합안에 따른 depth4 카테고리 명칭 중복제거
# 중복일 경우 상위 카테고리의 code 번호 선택(keep='first')
unique_key.drop_duplicates(subset=['depth3', 'depth4'], keep='first', inplace=True, ignore_index=False)

final_file = final_data_long_not0.merge(unique_key[['depth3', 'depth4', 'code']], left_on='category', right_on='depth4',
                                        how='left')

# 카테고리 유니크키 조인
Final_DA_MKTS_subService = final_file[final_file['code'].notnull()][['uida', 'code', 'scoring', 'quantile', 'type']]

Final_DA_MKTS_subService = Final_DA_MKTS_subService[Final_DA_MKTS_subService['uida'].isin(compulsion_list) == False]
Final_DA_MKTS_subService['dt'] = add_dt
Final_DA_MKTS_subService.rename(columns={'code': 'unique_key'}, inplace=False)
