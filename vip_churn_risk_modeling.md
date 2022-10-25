```python
%pyspark

cust_info = spark.sql("""

    with cust_info_base_table as (
        select a.dpstr_cust_no
             , max(case when a.memb_sex_cl_cd = '1' then 0
                        when a.memb_sex_cl_cd = '2' then 1
                        end) as sex_cd
             , max(case when a.age_cd between 0 and 19 then 1
                        when a.age_cd between 20 and 29 then 2
                        when a.age_cd between 30 and 39 then 3
                        when a.age_cd between 40 and 49 then 4
                        when a.age_cd between 50 and 59 then 5
                        when a.age_cd between 60 and 69 then 6
                        when a.age_cd between 70 and 79 then 7
                        else 8 end) as agrng_cd
             , max(case when substring(a.excll_cust_grde_smcls_cd, 1, 4) in ('1111', '1112', '1113', '1119') then '1.AVENUEL'
                        when substring(a.excll_cust_grde_smcls_cd, 1, 4) = '1221' then '2.MVG_L'
                        when substring(a.excll_cust_grde_smcls_cd, 1, 4) = '1222' then '3.MVG_P'
                        when substring(a.excll_cust_grde_smcls_cd, 1, 4) = '1223' then '4.MVG_C'
                        when substring(a.excll_cust_grde_smcls_cd, 1, 4) in ('1224', '1225', '1226') then '5.MVG_A'
                        when substring(a.excll_cust_grde_smcls_cd, 1, 4) = '1551' then '6.VIP_PLUS'
                        when substring(a.excll_cust_grde_smcls_cd, 1, 4) = '1552' then '7.VIP'
                        end) as cust_grde
             , max(case when a.cust_rel_step_cd = '~' then '99' else a.cust_rel_step_cd end) as cust_rel_step_cd
             , max(case when a.cust_evl_grde_cd = '~' then '99' else a.cust_evl_grde_cd end) as cust_evl_grde_cd
             , ifnull(max(case when a.dpstr_mustr_cd not in ('#', '~') then a.dpstr_mustr_cd end), '0999') as dpstr_mustr_cd
             , ifnull(max(case when a.otlt_mustr_cd not in ('#', '~') then a.otlt_mustr_cd end), '0999') as otlt_mustr_cd
             , max(a.sms_rcv_agr_yn) as sms_rcv_agr_yn
             , max(a.dpstr_app_push_agr_yn) as dpstr_app_push_agr_yn
        from lpciddm.tb_dmcs_mmcustinfo_f a
        join lpciddm.tb_dmct_excllcustsmcls_c b
            on a.excll_cust_grde_smcls_cd = b.excll_cust_grde_smcls_cd
        where 1 = 1
            and a.std_ym = date_format(current_date(), 'yyyyMM')
            and a.excll_cust_self_cl_cd = '1'
        group by 1
    )
    select t.*
    from cust_info_base_table t
    where 1 = 1
        and t.sex_cd is not null
        and t.agrng_cd is not null
        and t.cust_grde is not null
    
    """)

cust_info.createOrReplaceTempView("cust_info")
cust_info.cache()
cust_info.show(5)


%pyspark

# 최근 N개월 가전가구 구매비중 ==
# 최근 N개월 패션 구매비중 ==
# 최근 N개월 백화점 구매비중 ==
# 최근 N개월 아울렛 구매비중 ==
# 최근 N개월 롯데백화점몰 전체 구매비중 ==

# 최근 N개월 가전가구 구매일수 ==
# 최근 N개월 패션 구매일수 ==
# 최근 N개월 구매일수 ==
# 최근 N개월 롯데백화점몰 전체 구매일수

# 최근 N개월 구매주기 ==
# 최근 N개월 가전가구 구매주기
# 최근 N개월 패션 구매주기
# 최근 N개월 백화점 구매주기
# 최근 N개월 아울렛 구매주기
# 최근 N개월 롯데백화점몰 구매주기

# 종료_엘롯데(18.10)     012
# 종료_프리미엄몰(20.04) 018
# 롯데백화점몰           019
# 종료_프리미엄몰(20.11) 01A
# 롯데ON                 01B
# 종료_롯데ON(20.11)P    01C
        
purchase_info1 = spark.sql("""

    select a.dpstr_cust_no
    
        /* 1/2/3년전 가전/가구상품군 구매비중 */
         , round(ifnull(sum(case when ((a.std_ym between '202012' and '202111') and (c.buy_fld_cd in ('13', '14'))) then a.gs_slng_amt end) / sum(case when (a.std_ym between '202012' and '202111') then a.gs_slng_amt end), 0) * 100, 4) as pch_rt_lfst_he_y1     
         , round(ifnull(sum(case when ((a.std_ym between '201912' and '202011') and (c.buy_fld_cd in ('13', '14'))) then a.gs_slng_amt end) / sum(case when (a.std_ym between '201912' and '202011') then a.gs_slng_amt end), 0) * 100, 4) as pch_rt_lfst_he_y2
         , round(ifnull(sum(case when ((a.std_ym between '201812' and '201911') and (c.buy_fld_cd in ('13', '14'))) then a.gs_slng_amt end) / sum(case when (a.std_ym between '201812' and '201911') then a.gs_slng_amt end), 0) * 100, 4) as pch_rt_lfst_he_y3

        /* 1/2/3년전 패션상품군 구매비중 */
         , round(ifnull(sum(case when ((a.std_dt between '202012' and '202111') and (c.buy_fld_cd in ('16', '17'))) then a.gs_slng_amt end) / sum(case when (a.std_ym between '202012' and '202111') then a.gs_slng_amt end), 0) * 100, 4) as pch_rt_fs_y1
         , round(ifnull(sum(case when ((a.std_dt between '201912' and '202011') and (c.buy_fld_cd in ('16', '17'))) then a.gs_slng_amt end) / sum(case when (a.std_ym between '201912' and '202011') then a.gs_slng_amt end), 0) * 100, 4) as pch_rt_fs_y2
         , round(ifnull(sum(case when ((a.std_dt between '201812' and '201911') and (c.buy_fld_cd in ('16', '17'))) then a.gs_slng_amt end) / sum(case when (a.std_ym between '201812' and '201911') then a.gs_slng_amt end), 0) * 100, 4) as pch_rt_fs_y3

        /* 1/2/3년전 가전/가구 구매일수 */
         , ifnull(count(distinct case when ((a.std_ym between '202012' and '202111') and (c.buy_fld_cd in ('13', '14'))) then a.std_dt end), 0) as pch_dys_lfst_he_y1
         , ifnull(count(distinct case when ((a.std_ym between '201912' and '202011') and (c.buy_fld_cd in ('13', '14'))) then a.std_dt end), 0) as pch_dys_lfst_he_y2
         , ifnull(count(distinct case when ((a.std_ym between '201812' and '201911') and (c.buy_fld_cd in ('13', '14'))) then a.std_dt end), 0) as pch_dys_lfst_he_y3

        /* 1/2/3년전 패션상품군 구매일수 */
         , ifnull(count(distinct case when ((a.std_ym between '202012' and '202111') and (c.buy_fld_cd in ('16', '17'))) then a.std_dt end), 0) as pch_dys_fs_y1
         , ifnull(count(distinct case when ((a.std_ym between '201912' and '202011') and (c.buy_fld_cd in ('16', '17'))) then a.std_dt end), 0) as pch_dys_fs_y2
         , ifnull(count(distinct case when ((a.std_ym between '201812' and '201911') and (c.buy_fld_cd in ('16', '17'))) then a.std_dt end), 0) as pch_dys_fs_y3

    from lpciddm.tb_dmcs_dditmnocustitgslngdtl_f a
    join lpciddm.tb_dmcs_mmcustinfo_f b
        on a.dpstr_cust_no = b.dpstr_cust_no
        and a.std_ym = b.std_ym
    join lpciddw.tb_dwbs_stritmno_m c
        on a.cstr_cd = c.cstr_cd
        and a.itmno_cd = c.itmno_cd
    where 1 = 1
        and a.std_ym between '201812' and '202111'
        and a.on_off_cl_cd = '1'
    group by 1
    
    """)

purchase_info1.createOrReplaceTempView("purchase_info1")
purchase_info1.cache()
purchase_info1.show(5)


%pyspark

purchase_info2 = spark.sql("""

    select a.dpstr_cust_no
    
        /* 1/2/3년전 백화점 구매비중 */
         , round(ifnull(sum(case when ((a.std_ym between '202012' and '202111') and (a.typbu_dtl_cd = '01')) then a.gs_slng_amt end) / sum(case when (a.std_ym between '202012' and '202111') then a.gs_slng_amt end), 0) * 100, 4) as pch_rt_dpstr_y1
         , round(ifnull(sum(case when ((a.std_ym between '201912' and '202011') and (a.typbu_dtl_cd = '01')) then a.gs_slng_amt end) / sum(case when (a.std_ym between '201912' and '202011') then a.gs_slng_amt end), 0) * 100, 4) as pch_rt_dpstr_y2
         , round(ifnull(sum(case when ((a.std_ym between '201812' and '201911') and (a.typbu_dtl_cd = '01')) then a.gs_slng_amt end) / sum(case when (a.std_ym between '201812' and '201911') then a.gs_slng_amt end), 0) * 100, 4) as pch_rt_dpstr_y3
        
        /* 1/2/3년전 아울렛 구매비중 */
         , round(ifnull(sum(case when ((a.std_ym between '202012' and '202111') and (a.typbu_dtl_cd = '02')) then a.gs_slng_amt end) / sum(case when (a.std_ym between '202012' and '202111') then a.gs_slng_amt end), 0) * 100, 4) as pch_rt_otlt_y1
         , round(ifnull(sum(case when ((a.std_ym between '201912' and '202011') and (a.typbu_dtl_cd = '02')) then a.gs_slng_amt end) / sum(case when (a.std_ym between '201912' and '202011') then a.gs_slng_amt end), 0) * 100, 4) as pch_rt_otlt_y2
         , round(ifnull(sum(case when ((a.std_ym between '201812' and '201911') and (a.typbu_dtl_cd = '02')) then a.gs_slng_amt end) / sum(case when (a.std_ym between '201812' and '201911') then a.gs_slng_amt end), 0) * 100, 4) as pch_rt_otlt_y3

        /* 1/2/3년전 롯데백화점몰 구매비중 */
         , round(ifnull(sum(case when ((a.std_ym between '202012' and '202111') and (a.typbu_dtl_cd in ('012', '018', '019', '01A', '01B', '01C'))) then a.gs_slng_amt end) / sum(case when (a.std_ym between '202012' and '202111') then a.gs_slng_amt end), 0) * 100, 4) as pch_rt_mall_y1
         , round(ifnull(sum(case when ((a.std_ym between '201912' and '202011') and (a.typbu_dtl_cd in ('012', '018', '019', '01A', '01B', '01C'))) then a.gs_slng_amt end) / sum(case when (a.std_ym between '201912' and '202011') then a.gs_slng_amt end), 0) * 100, 4) as pch_rt_mall_y2
         , round(ifnull(sum(case when ((a.std_ym between '201812' and '201911') and (a.typbu_dtl_cd in ('012', '018', '019', '01A', '01B', '01C'))) then a.gs_slng_amt end) / sum(case when (a.std_ym between '201812' and '201911') then a.gs_slng_amt end), 0) * 100, 4) as pch_rt_mall_y3

        /* 1/2/3년전 구매일수 */
         , ifnull(count(distinct case when a.std_ym between '202012' and '202111' then a.std_dt end), 0) as pch_dys_y1
         , ifnull(count(distinct case when a.std_ym between '201912' and '202011' then a.std_dt end), 0) as pch_dys_y2
         , ifnull(count(distinct case when a.std_ym between '201812' and '201911' then a.std_dt end), 0) as pch_dys_y3
         
        /* 1/2/3년전 롯데백화점몰 구매일수 */
         , ifnull(count(distinct case when ((a.std_ym between '202012' and '202111') and (a.typbu_dtl_cd in ('012', '018', '019', '01A', '01B', '01C'))) then a.std_dt end), 0) as pch_dys_mall_y1
         , ifnull(count(distinct case when ((a.std_ym between '201912' and '202011') and (a.typbu_dtl_cd in ('012', '018', '019', '01A', '01B', '01C'))) then a.std_dt end), 0) as pch_dys_mall_y2
         , ifnull(count(distinct case when ((a.std_ym between '201812' and '201911') and (a.typbu_dtl_cd in ('012', '018', '019', '01A', '01B', '01C'))) then a.std_dt end), 0) as pch_dys_mall_y3

    from lpciddm.tb_dmcs_dditmnocustitgslngdtl_f a
    join lpciddm.tb_dmcs_mmcustinfo_f b
        on a.dpstr_cust_no = b.dpstr_cust_no
        and a.std_ym = b.std_ym
    join lpciddw.tb_dwbs_stritmno_m c
        on a.cstr_cd = c.cstr_cd
        and a.itmno_cd = c.itmno_cd
    where 1 = 1
        and a.std_ym between '201812' and '202111'
        and a.on_off_cl_cd = '1'
    group by 1
    
    """)

purchase_info2.createOrReplaceTempView("purchase_info2")
purchase_info2.cache()
purchase_info2.show(5)


%pyspark

# 최근 N개월 구매주기 ==
# 최근 N개월 가전가구 구매주기
# 최근 N개월 패션 구매주기
# 최근 N개월 백화점 구매주기
# 최근 N개월 아울렛 구매주기
# 최근 N개월 롯데백화점몰 구매주기

buy_fld_cd = ['13', '14', '16', '17']
typbu_dtl_cd = ['01', '02']
typbu_dtl_cd_mall = ['012', '018', '019', '01A', '01B', '01C']

purchase_cyc = spark.sql("""
    
    /* 최근 1/2/3년 구매주기 */
    with base as (
        select a.dpstr_cust_no
             , min(case when (a.std_ym between '202012' and '202111') then a.std_dt end) as min_dys_y1
             , min(case when (a.std_ym between '201912' and '202011') then a.std_dt end) as min_dys_y2             
             , min(case when (a.std_ym between '201812' and '201911') then a.std_dt end) as min_dys_y3

             , max(case when (a.std_ym between '202012' and '202111') then a.std_dt end) as max_dys_y1
             , max(case when (a.std_ym between '201912' and '202011') then a.std_dt end) as max_dys_y2             
             , max(case when (a.std_ym between '201812' and '201911') then a.std_dt end) as max_dys_y3
             
             , count(distinct case when (a.std_ym between '202012' and '202111') then a.std_dt end) as pch_dys_y1
             , count(distinct case when (a.std_ym between '201912' and '202011') then a.std_dt end) as pch_dys_y2             
             , count(distinct case when (a.std_ym between '201812' and '201911') then a.std_dt end) as pch_dys_y3
             
             , datediff(max(case when (a.std_ym between '202012' and '202111') then a.std_dt end), min(case when (a.std_ym between '202012' and '202111') then a.std_dt end)) as diff_y1
             , datediff(max(case when (a.std_ym between '201912' and '202011') then a.std_dt end), min(case when (a.std_ym between '201912' and '202011') then a.std_dt end)) as diff_y2
             , datediff(max(case when (a.std_ym between '201812' and '201911') then a.std_dt end), min(case when (a.std_ym between '201812' and '201911') then a.std_dt end)) as diff_y3
             
        from lpciddm.tb_dmcs_dditmnocustitgslngdtl_f a
        join lpciddm.tb_dmcs_mmcustinfo_f b
            on a.dpstr_cust_no = b.dpstr_cust_no
            and a.std_ym = b.std_ym
        where 1 = 1
            and a.std_ym between '201812' and '202111'
            and a.on_off_cl_cd = '1'
        group by 1
    
    )
    select t.dpstr_cust_no
         , ifnull(round(case when t.pch_dys_y1 < 2 then null else (t.diff_y1 / (t.pch_dys_y1 - 1)) end, 4) ,0) as pch_cyc_y1
         , ifnull(round(case when t.pch_dys_y2 < 2 then null else (t.diff_y2 / (t.pch_dys_y2 - 1)) end, 4) ,0) as pch_cyc_y2
         , ifnull(round(case when t.pch_dys_y3 < 2 then null else (t.diff_y3 / (t.pch_dys_y3 - 1)) end, 4) ,0) as pch_cyc_y3
    from base t
    
    """)

purchase_cyc.createOrReplaceTempView("purchase_cyc")
purchase_cyc.cache()
purchase_cyc.show(5)

%pyspark

# 최근 N개월 구매주기 ==
year_start = ['202012', '201912', '201812']; year_end = ['202111', '202011', '201911']; year = ['y1', 'y2', 'y3']
purchase_cyc_li1 = []; df_final_li1 = []

for start, end, yr in zip(year_start, year_end, year):
    purchase_cyc1 = spark.sql(f"""
        with base as (
            select a.dpstr_cust_no
                 , min(case when (a.std_ym between {start} and {end}) then a.std_dt end) as min_dys_{yr}
                 , max(case when (a.std_ym between {start} and {end}) then a.std_dt end) as max_dys_{yr}
                 , count(distinct case when (a.std_ym between {start} and {end}) then a.std_dt end) as pch_dys_{yr}
                 , datediff(max(case when (a.std_ym between {start} and {end}) then a.std_dt end), min(case when (a.std_ym between {start} and {end}) then a.std_dt end)) as diff_{yr}
            from lpciddm.tb_dmcs_dditmnocustitgslngdtl_f a
            join lpciddm.tb_dmcs_mmcustinfo_f b
                on a.dpstr_cust_no = b.dpstr_cust_no
                and a.std_ym = b.std_ym
            where 1 = 1
                and a.std_ym between {start} and {end}
                and a.on_off_cl_cd = '1'
            group by 1
        )
        select t.dpstr_cust_no
             , ifnull(round(case when t.pch_dys_{yr} < 2 then null else (t.diff_{yr} / (t.pch_dys_{yr} - 1)) end, 4), 0) as pch_cyc_{yr}
        from base t
        """)
    purchase_cyc_li1.append(purchase_cyc1)

for i, df in enumerate(purchase_cyc_li): 
    if i == 0: 
        df_temp = df
    else: 
        df_temp = df_temp.join(df, on=['dpstr_cust_no'], how='left_outer')

df_final_li1.append(df_temp)
df_final1 = df_final_li1[0].na.fill(0)
df_final1.show(5)

%pyspark

# 최근 N개월 가전가구 및 패션상품군 구매주기
year_start = ['202012', '201912', '201812']; year_end = ['202111', '202011', '201911']; year = ['y1', 'y2', 'y3']
purchase_cyc_li2 = []; df_final_li2 = []
buy_fld_cd = ['13', '14', '16', '17']

for start, end, yr in zip(year_start, year_end, year):
    for fld in buy_fld_cd: 
        purchase_cyc2 = spark.sql(f"""
            with base as (
                select a.dpstr_cust_no
                     , min(case when ((a.std_ym between {start} and {end}) and (c.buy_fld_cd = {fld})) then a.std_dt end) as min_dys_{fld}_{yr}
                     , max(case when ((a.std_ym between {start} and {end}) and (c.buy_fld_cd = {fld})) then a.std_dt end) as max_dys_{fld}_{yr}
                     , count(distinct case when ((a.std_ym between {start} and {end}) and (c.buy_fld_cd = {fld})) then a.std_dt end) as pch_dys_{fld}_{yr}
                     , datediff(max(case when ((a.std_ym between {start} and {end}) and (c.buy_fld_cd = {fld})) then a.std_dt end), min(case when ((a.std_ym between {start} and {end}) and (c.buy_fld_cd = {fld})) then a.std_dt end)) as diff_{fld}_{yr}
                from lpciddm.tb_dmcs_dditmnocustitgslngdtl_f a
                join lpciddm.tb_dmcs_mmcustinfo_f b
                    on a.dpstr_cust_no = b.dpstr_cust_no
                    and a.std_ym = b.std_ym
                join lpciddw.tb_dwbs_stritmno_m c
                    on a.cstr_cd = c.cstr_cd
                    and a.itmno_cd = c.itmno_cd
                where 1 = 1
                    and a.std_ym between {start} and {end}
                    and a.on_off_cl_cd = '1'
                group by 1
            )
            select t.dpstr_cust_no
                 , ifnull(round(case when t.pch_dys_{fld}_{yr} < 2 then null else (t.diff_{fld}_{yr} / (t.pch_dys_{fld}_{yr} - 1)) end, 4), 0) as pch_cyc_{fld}_{yr}
            from base t
            """)
        purchase_cyc_li2.append(purchase_cyc2)
        
    for i, df in enumerate(purchase_cyc_li):
        if i == 0:
            df_temp = df
        else:
            df_temp = df_temp.join(df, on=['dpstr_cust_no'], how='left_outer')

df_final_li2.append(df_temp)
df_final2 = df_final_li2[0].na.fill(0)
df_final2.show(5)

%pyspark

# 최근 N개월 백화점/아울렛/롯데백화점몰 구매주기
year_start = ['202012', '201912', '201812']; year_end = ['202111', '202011', '201911']; year = ['y1', 'y2', 'y3']
purchase_cyc_li3 = []; df_final_li3 = []
typbu_dtl_cd = ['01', '02', '012', '018', '019', '01A', '01B', '01C']

for start, end, yr in zip(year_start, year_end, year):
    for typ in typbu_dtl_cd: 
        purchase_cyc3 = spark.sql(f"""
            with base as (
                select a.dpstr_cust_no
                     , min(case when ((a.std_ym between {start} and {end}) and (a.typbu_dtl_cd = '{typ}')) then a.std_dt end) as min_dys_{typ}_{yr}
                     , max(case when ((a.std_ym between {start} and {end}) and (a.typbu_dtl_cd = '{typ}')) then a.std_dt end) as max_dys_{typ}_{yr}
                     , count(distinct case when ((a.std_ym between {start} and {end}) and (a.typbu_dtl_cd = '{typ}')) then a.std_dt end) as pch_dys_{typ}_{yr}
                     , datediff(max(case when ((a.std_ym between {start} and {end}) and (a.typbu_dtl_cd = '{typ}')) then a.std_dt end), min(case when ((a.std_ym between {start} and {end}) and (a.typbu_dtl_cd = '{typ}')) then a.std_dt end)) as diff_{typ}_{yr}
                from lpciddm.tb_dmcs_dditmnocustitgslngdtl_f a
                join lpciddm.tb_dmcs_mmcustinfo_f b
                    on a.dpstr_cust_no = b.dpstr_cust_no
                    and a.std_ym = b.std_ym
                where 1 = 1
                    and a.std_ym between {start} and {end}
                    and a.on_off_cl_cd = '1'
                group by 1
            )
            select t.dpstr_cust_no
                 , ifnull(round(case when t.pch_dys_{typ}_{yr} < 2 then null else (t.diff_{typ}_{yr} / (t.pch_dys_{typ}_{yr} - 1)) end, 4), 0) as pch_cyc_{typ}_{yr}
            from base t
            """)
        purchase_cyc_li3.append(purchase_cyc3)
        
    for i, df in enumerate(purchase_cyc_li):
        if i == 0:
            df_temp = df
        else:
            df_temp = df_temp.join(df, on=['dpstr_cust_no'], how='left_outer')

df_final_li3.append(df_temp)
df_final3 = df_final_li3[0].na.fill(0)
df_final3.show()




```
