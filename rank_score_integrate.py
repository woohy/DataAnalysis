import pandas as pd
import numpy as np
import datetime
from pandasql import sqldf
pysqldf = lambda q: sqldf(q, globals())

#####################################################
# 유니크 카테고리 가져오기
#####################################################
unique_key = pd.read_csv('file_name', encoding='euc-kr',
                         dtype={'depth1_num_code':str, 'depth2_num_code':str, 'depth3_num_code':str, 'depth4_num_code':str, 'code':str
                                ,'depth1':str, 'depth2':str, 'depth3':str, 'depth4':str})

scoring_1 = pd.read_csv(f'file_name_{add_dt}.csv', encoding='euc-kr',
                        dtype={'uida':str, 'code':str, 'scoring':str, 'quantile':str,'type':str,'click':str,'pageview':str,'dt':str})
scoring_2 = pd.read_csv(f'file_name_{add_dt_bart}.csv', encoding='euc-kr',
                        dtype={'uida':str, 'unique_key':str, 'type':str, 'scoring':str,'quatile':str,'click':str,'pageview':str})
unique_key = pd.DataFrame(unique_key)
scoring_1 = pd.DataFrame(scoring_1)
scoring_2 = pd.DataFrame(scoring_2)


#####################################################
# uida, unique_key별 pageview, click 수, scoring 점수 합산
#####################################################
q1 = """SELECT uida,
               unique_key,
               SUM(pageview) AS pageview,
               SUM(click) AS click,
               SUM(scoring) AS scoring
       FROM scoring_total
       GROUP BY uida, unique_key
       """

scoring_total = pysqldf(q1)

scoring_total['pvck'] = scoring_total['pageview'] + scoring_total['click']

#####################################################
# 우선순위(rank) 계산: pvck가 클수록, pvck가 동일하다면 scoring이 클수록 상위 rank 할당
# pvck == 1 인 유저는 rank_group 음수화를 위해 우선순위 별도로 할당
#####################################################
scoring_total['is_noactive'] = scoring_total['pvck'].apply(lambda x: 1 if x <= 1 else 0)
scoring_total.sort_values(['pvck', 'scoring'], ascending=[False, False], inplace=True)
scoring_total['rank'] = 1
scoring_total['rank'] = scoring_total.groupby(['unique_key', 'is_noactive'])['rank'].cumsum()

# 0등 부터 시작
scoring_total['rank'] = scoring_total['rank']-1
scoring_total['rank_group'] = np.where((scoring_total['is_noactive'] == 1), -1*((scoring_total['rank']//10000)+1), (scoring_total['rank']//10000)+1)

#####################################################
# score 및 quantile 계산 코드
#####################################################
scoring_total['total_score'] = 200
scoring_total['ratio'] = round((scoring_total['scoring'] / scoring_total['total_score']),2)
scoring_total['score_scaling'] = round(scoring_total['ratio'] * 100)
scoring_total['score_scaling'] = scoring_total['score_scaling'].astype(float)
scoring_total['score_scaling'] = scoring_total['score_scaling'].astype(int)
scoring_total['quantile'] = np.where((scoring_total['score_scaling'] >= 0) & (scoring_total['score_scaling'] < 10), 5,
                                  np.where((scoring_total['score_scaling'] >= 10) & (scoring_total['score_scaling'] < 20), 4,
                                          np.where((scoring_total['score_scaling'] >= 20) & (scoring_total['score_scaling'] < 30), 3,
                                                  np.where((scoring_total['score_scaling'] >= 30) & (scoring_total['score_scaling'] < 40), 2, 1))))


scoring_total['dt'] = add_dt
scoring_total.drop(columns=['total_score', 'ratio'], inplace=True)
scoring_total = scoring_total[['uida','unique_key', 'pageview', 'click', 'pvck', 'rank', 'rank_group', 'score_scaling', 'quantile', 'dt']]


#####################################################
# rank_group, 카테고리별 UIDA 수 계산
#####################################################
q2 = """SELECT B.CODE,
               A.RANK_GROUP,
               B.DEPTH1,
               B.DEPTH2,
               B.DEPTH3,
               B.DEPTH4,
               A.COUNT
        FROM
        (
        SELECT unique_key,
               rank_group AS RANK_GROUP,
               COUNT(uida) AS COUNT
        FROM scoring_total_final
        GROUP BY unique_key, rank_group
       ) A
       INNER JOIN (
                  SELECT *
                  FROM unique_key
                  ) B ON A.UNIQUE_KEY = B.CODE
       """

scoring_total_final_v2 = pysqldf(q2)

