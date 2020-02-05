import pandas as pd
import sqlalchemy
import datetime as dt
import numpy as np
import cx_Oracle
from datetime import datetime
import oraclecon as con

# sample engine string oracle+cx_oracle://username:password$@server:port/?service_name=servicename?charset=utf8
engine = sqlalchemy.create_engine(con.strengine)

# Using NVL to add a value to empty fields so the column can be used in
# indexing for the pivot. Could be done in other ways but this seemed simplest.
strsql = '''select 
b.DOC_HDR_ID, a.DOC_TYP_NM, a.DOC_CRTE_DT, b.CRTE_DT, a.DOC_MDFN_DT, b.ACTN_RQST_CD,
    NVL(c.PRNCPL_NM, '0') as PRNCPL_NM, NVL(d.GRP_NM,'0') as GRP_NM, 
    NVL(b.QUAL_ROLE_NM,'0') as QUAL_ROLE_NM,
    a.FLD_NM, a.FLD_VAL
from
DSS_KR.KR_KREW_ACTN_RQST_T_v b
left join dss_kr.kr_edl_dmp_gt a on b.doc_hdr_id = a.doc_hdr_id
left join DSS_KR.KR_KRIM_PRNCPL_T_GT c on b.PRNCPL_ID = c.PRNCPL_ID
left join DSS_KR.KR_KRIM_GRP_T_GT d on b.GRP_ID = d.GRP_ID
where
a.DOC_RTE_STAT_CD = 'R'
and
b.PARNT_ID is null
and
b.ACTN_TKN_ID is null
and
a.FLD_NM in ('campus','campusTitle','concentration1','concentration2','concentrationTrack',
             'department','department1','department2','major','major1','major2','minor1','minor2')
order by b.DOC_HDR_ID, b.CRTE_DT'''

df = pd.read_sql_query(strsql, engine)
df['doc_crte_dt'] = df['doc_crte_dt'].dt.strftime('%d-%b-%y')
df['crte_dt'] = df['crte_dt'].dt.strftime('%d-%b-%y')
df['doc_mdfn_dt'] = df['doc_mdfn_dt'].dt.strftime('%d-%b-%y')
df.to_csv('C:\\Users\\ejnic\\Google Drive Personal\\Python\\enrouteedocs\\files\\enrouteedocs.csv')

# Pivot to get column values specific to docid/doctypes
dfpivot = pd.pivot_table(df, index = ['doc_hdr_id', 'doc_typ_nm', 'doc_crte_dt', 'crte_dt', 'doc_mdfn_dt',
                        'actn_rqst_cd', 'prncpl_nm', 'grp_nm', 'qual_role_nm'],
                         columns = ['fld_nm'], values = 'fld_val', aggfunc = 'first',
                         fill_value = '0')

# some of this could be done in the SQL but it is more transparent to
# specify exactly what and why below

# drop irrelevant doctypes
dfpivot = dfpivot.reset_index()
dfpivot = dfpivot[(dfpivot.doc_typ_nm != 'UGS.StudyAbroad.Doctype') &
                (dfpivot.doc_typ_nm != 'UGS.AddRemoveUserRequest.Doctype') &
                (dfpivot.doc_typ_nm != 'UGS.Hospitality.Doctype')]

# give codes words
dfpivot['actn_rqst_cd'] = dfpivot['actn_rqst_cd'].str.replace('A', 'Approve')
dfpivot['actn_rqst_cd'] = dfpivot['actn_rqst_cd'].str.replace('F', 'FYI')


# fix BL campus before eDocs were available to IUPUI
pattern = '|'.join(['0', 'select', 'Select'])
dfpivot['campus'] = dfpivot['campus'].str.replace(pattern, 'BL')


# consolidate requested of, group name, prncpl_nm and qual_role_nm
dfpivot['requestedof'] = '0'
dfpivot['requestedof'] = dfpivot['grp_nm']
dfpivot.loc[~dfpivot['prncpl_nm'].str.contains('0'), 'requestedof'] = dfpivot['prncpl_nm']
dfpivot.loc[~dfpivot['qual_role_nm'].str.contains('0'), 'requestedof'] = dfpivot['qual_role_nm']

# get rid of selects
pattern = '|'.join(['0', 'select', 'Select'])
dfpivot = dfpivot.replace(to_replace = ['Select', 'select', '0', 'none'], value = 'NA')

# move masters info into the correct column
#enroute$department1[which(is.na(enroute$department1))] <- enroute$department[which(is.na(enroute$department1))]
dfpivot.loc[dfpivot['department1'].str.contains('NA'), 'department1'] = dfpivot['department']

# move major into major1
dfpivot.loc[dfpivot['major1'].str.contains('NA'), 'major1'] = dfpivot['major']

# move concentrationTrack into concentration1
dfpivot.loc[dfpivot['concentration1'].str.contains('NA'), 'concentration1'] = dfpivot['concentrationTrack']

# re-order columns and drop those not needed
dfpivot.drop(['prncpl_nm', 'qual_role_nm', 'grp_nm', 'campusTitle','department',
              'concentrationTrack','major'], axis=1, inplace=True)

# change all NA's to empty string
dfpivot = dfpivot.replace(to_replace = 'NA', value = '')

# take campus off dept ID
dfpivot = dfpivot.replace(to_replace = [r'BL-', r'IN-'], value = '', regex=True)

# calculate age
currentdate = datetime.now().date()
dfpivot['dayssincerequest'] = (currentdate - pd.to_datetime(dfpivot['crte_dt']).dt.date).dt.days
dfpivot['dayssincecreate'] = (currentdate - pd.to_datetime(dfpivot['doc_crte_dt']).dt.date).dt.days

# write out file for consumption


dfpivot.to_csv('C:\\Users\\ejnic\\Google Drive Personal\\Python\\enrouteedocs\\files\\dfpivot.csv')
print(dfpivot.dtypes)

#pd.pivot_table(df,index=["date", "id"], columns="test", values="result", aggfunc= 'first').reset_index().rename_axis(None, 1)

#'doc_hdr_id','doc_typ_nm','doc_crte_dt','crte_dt','doc_mdfn_dt','actn_rqst_cd','prncpl_nm','grp_nm','qual_role_nm'


#print(df.("b.CRTE_DT"))

#print(df.dtypes)
