create or replace PACKAGE        zdw_employee_dedn_extr IS
--
  TYPE tab_dedn_extr IS TABLE OF ZTT_EMPLOYEE_DEDN_INPUT%ROWTYPE;
--
  FUNCTION f_get_employee_dedn(event_in VARCHAR2,
                          calendar_year_in VARCHAR2,
                          calendar_month_in VARCHAR2,
                          multi_source_group_in VARCHAR2)
                          RETURN tab_dedn_extr PIPELINED;
--
END zdw_employee_dedn_extr;

create or replace PACKAGE BODY        zdw_employee_dedn_extr IS
/******************************************************************************/
  FUNCTION f_get_employee_dedn    (event_in VARCHAR2,
                          calendar_year_in VARCHAR2,
                          calendar_month_in VARCHAR2,
                          multi_source_group_in VARCHAR2)
                          RETURN tab_dedn_extr PIPELINED IS
--
    ret_row                       ZTT_EMPLOYEE_DEDN_INPUT%ROWTYPE;
    ret_row_init                  ZTT_EMPLOYEE_DEDN_INPUT%ROWTYPE;
--
    dummy                         VARCHAR2(255);
    mif_cleanse_value             VARCHAR2(255);
    null_cleanse_value            VARCHAR2(255);
    event_date_value              DATE := SYSDATE;
    eom_date_value                DATE;
    to_num_month                  NUMBER := TO_NUMBER(calendar_month_in);
    pers_demo_rec                 EDW_GENERAL_EXTR.GET_PERSON_DEMO%ROWTYPE;
    pers_demo_rec_init            EDW_GENERAL_EXTR.GET_PERSON_DEMO%ROWTYPE;
    race_rec                      EDW_GENERAL_EXTR.GET_RACE%ROWTYPE;
    race_rec_init                 EDW_GENERAL_EXTR.GET_RACE%ROWTYPE;
    visa_rec                      MST_VISA%ROWTYPE;
    visa_rec_init                 MST_VISA%ROWTYPE;
    visa_ra_rec                   EDW_GENERAL_EXTR.GET_RA_VISA%ROWTYPE;
    visa_ra_rec_init              EDW_GENERAL_EXTR.GET_RA_VISA%ROWTYPE;
    vettype_rec                   EDW_GENERAL_EXTR.GET_VETTYPE%ROWTYPE;
    vettype_rec_init              EDW_GENERAL_EXTR.GET_VETTYPE%ROWTYPE;
    sal_rate_rec                  MPT_SALARY_RATE%ROWTYPE;
    sal_rate_rec_init             MPT_SALARY_RATE%ROWTYPE;
    faculty_rec                   MST_FACULTY%ROWTYPE;
    faculty_rec_init              MST_FACULTY%ROWTYPE;
--
    CURSOR get_last_day_of_month IS
      SELECT LAST_DAY(TO_DATE('01'|| calendar_month_in || calendar_year_in,'DDMMYYYY'))
     --select last_day(to_date('01012015','DDMMYYYY'))
        FROM DUAL;
--
/*** Bryan - you are going to use this to build the "dedn_type" field -- load records to MTVPARM to classify each **/
/*** Will need input from HR as to each benefit fitting into a bucket */
    CURSOR get_mtvparm(int_code_group VARCHAR2, int_code VARCHAR2, int_code2 VARCHAR2) IS
      SELECT MTVPARM_EXTERNAL_CODE,
             MTVPARM_DESC
        FROM MTVPARM
       WHERE MTVPARM_INTERNAL_CODE_GROUP = int_code_group
         AND MTVPARM_INTERNAL_CODE = int_code
         AND (MTVPARM_INTERNAL_CODE_2 = int_code2 or int_code2 = 'ALL');
--
/******************************************************************/
/*************** PUT MAIN TABLE FUNCTION HERE *********************/
/******************************************************************/
   cursor get_dedn_pop(multi_src_in varchar2) is
        select pd.person_uid           as person_uid,
              we.warehouse_entity_uid as edw_pidm,
              pd.mif_value            as multi_source,
              pd.calendar_year        as calendar_year,
              --pd.payroll_identifier   as payroll_identifier,
              pd.deduction            as carrier,
              --nvl(f_get_carrier_coverage(pd.person_uid ,to_date('31-AUG-2012','DD-MON-YY'),pd.mif_value ,pd.deduction ),'None'),
              nvl(odsmgr.zpkfunc.f_get_carrier_coverage(pd.person_uid ,eom_date_value,pd.mif_value ,pd.deduction ),'None') as coverage_option,
              ptrbdca_short_desc as carrier_short_desc,
              pd.deduction_long_desc  as carrier_long_desc,
              to_char(zc.pay_date,'MM')  as calendar_month,
              e.employee_status,
              e.employee_class,
              e.employee_grouping,
              e.full_or_part_time_ind as employee_time_status,
              e.adjusted_service_date,
              e.original_hire_date,
              e.home_organization,
              e.home_organization_chart,
              e.campus,
              e.college,
              e.division,
              ep.employer_code,
              ep.full_time_equivalency_pct,
              ep.assignment_salary_group as assign_salary_group,
              ep.assignment_table        as assign_table,
              ep.assignment_grade        as assign_grade,
              f.eeo_skill,
              sal_tot.annual_salary,
              decode(ep.position_status,'A','Y','N') as active_position_ind,
              pd.zone_value,
              pd.domain_value
              ,sum(pd.employee_deduction_amount)        as employee_dedn_amt,
              sum(pd.employer_deduction_amount)         as employer_dedn_amt
        from mpt_payroll_deduction  pd,
            zpt_payroll_calendar   zc,
            mpt_employee           e,
            wdt_warehouse_entity   we,
            mpt_empl_position      ep,
            mpt_position_def       f,
            (select a.person_uid, 
                    a.mif_value,
                    sum(a.annual_salary) annual_salary,
                    sum(a.encumbrance_amount) encumbrance_amount
                from mpt_empl_position a
              where eom_date_value between a.effective_start_date and a.effective_end_date
                and eom_date_value between a.position_begin_date and nvl(a.position_end_date,'31-DEC-2099')
              --  where to_date('31-MAR-2012','DD-MON-YY') between a.effective_start_date and a.effective_end_date
                --and to_date('31-MAR-2012','DD-MON-YY') between a.position_begin_date and nvl(a.position_end_date,'31-DEC-2099')
                and a.position_contract_type <> 'O'
                and a.mif_value = multi_src_in
            group by a.person_uid, a.mif_value) sal_tot,
           (select ptrbdca_code, ptrbdca_short_desc, ptrbdca_vpdi_code from ptrbdca)  short_desc
        where pd.calendar_year      = zc.fiscal_year 
          and pd.payroll_number     = zc.payroll_number
          and pd.mif_value          = zc.mif_value
          and pd.payroll_identifier = zc.payroll_type
          and pd.person_uid         = e.person_uid
          and pd.mif_value          = e.mif_value
          and pd.person_uid         = we.banner_pidm
          and pd.mif_value          = we.user_attribute_01
          and pd.person_uid         = ep.person_uid
          and pd.mif_value          = ep.mif_value
          and f.position            = ep.position
          and f.mif_value           = ep.mif_value
          and pd.person_uid         = sal_tot.person_uid
          and pd.mif_value          = sal_tot.mif_value
          and pd.deduction          = short_desc.ptrbdca_code
          and pd.mif_value          = short_desc.ptrbdca_vpdi_code
          and ep.position_contract_type = 'P'
          and eom_date_value between ep.effective_start_date and ep.effective_end_date
          and eom_date_value between ep.position_begin_date and nvl(ep.position_end_date,'31-DEC-2099')
          --AND to_date('31-MAR-2012','DD-MON-YY') BETWEEN EP.EFFECTIVE_START_DATE AND EP.EFFECTIVE_END_DATE
          --AND to_date('31-MAR-2012','DD-MON-YY') BETWEEN EP.POSITION_BEGIN_DATE AND NVL(EP.POSITION_END_DATE,'31-DEC-2099')
          --and pd.person_uid = 1747659 
          and pd.calendar_year = calendar_year_in
        --and pd.calendar_year = '2015'
          and to_char(zc.pay_date,'MM') = calendar_month_in
         --and to_char(zc.pay_date,'MM') = '01'
          and pd.mif_value = multi_src_in
        group by pd.person_uid,
                we.warehouse_entity_uid,
                pd.mif_value,
                pd.calendar_year,
                --pd.payroll_identifier,
                pd.deduction,
                nvl(odsmgr.zpkfunc.f_get_carrier_coverage(pd.person_uid ,eom_date_value,pd.mif_value ,pd.deduction ),'None'),
                ptrbdca_short_desc,
                pd.deduction_long_desc,
                pd.calendar_year,
                to_char(pay_date,'MM'),
                e.employee_status,
                e.employee_class,
                e.employee_grouping,
                e.full_or_part_time_ind,
                e.adjusted_service_date,
                e.original_hire_date,
                e.home_organization,
                e.home_organization_chart,
                e.campus,
                e.college,
                e.division,
                ep.employer_code,
                ep.full_time_equivalency_pct,
                ep.assignment_salary_group,
                ep.assignment_table,
                ep.assignment_grade,
                f.eeo_skill,
                sal_tot.annual_salary,
                decode(ep.position_status,'A','Y','N'),
                pd.zone_value,
                pd.domain_value;

/*** Extra cursors we need to populated dims and fact */
   /* Need this to get tenure, faculty_member_category,  for employee dimension */
    CURSOR get_faculty(multi_src_in VARCHAR2, person_in NUMBER) IS
      SELECT *
        FROM MST_FACULTY
       WHERE PERSON_UID = person_in
         AND MIF_VALUE = multi_src_in;
    
    /* Need this to get salary_rate_ind, used for FTE counts */
    CURSOR get_sal_rate(multi_src_in VARCHAR2, group_in VARCHAR2, table_in VARCHAR2, grade_in VARCHAR2) IS
      SELECT *
        FROM MPT_SALARY_RATE
       WHERE SALARY_GROUP = group_in
         AND SALARY_TABLE = table_in
         AND SALARY_GRADE = grade_in
         AND NVL(MIF_VALUE, mif_cleanse_value) = multi_src_in;

/** Open all your cursors***/
--
  BEGIN
--
    OPEN get_last_day_of_month;
    FETCH get_last_day_of_month INTO eom_date_value;
    CLOSE get_last_day_of_month;
--
    OPEN get_mtvparm('CLEANSING DEFAULT VALUES','MULTI_SOURCE_CLEANSE_VALUE','ALL');
    FETCH get_mtvparm INTO
          dummy,
          mif_cleanse_value;
    CLOSE get_mtvparm;
--
    OPEN get_mtvparm('CLEANSING DEFAULT VALUES','NULL_CLEANSE_VALUE','ALL');
    FETCH get_mtvparm INTO
          dummy,
          null_cleanse_value;
    CLOSE get_mtvparm;
--
    FOR multi_src_rec IN get_mtvparm('EDW EXTRACT PARAMETERS','MULTI_SOURCE_GROUP',multi_source_group_in) LOOP
      FOR dedn_rec IN get_dedn_pop(multi_src_rec.mtvparm_external_code) LOOP
--
        OPEN edw_general_extr.get_person_demo(mif_cleanse_value, multi_src_rec.mtvparm_external_code, dedn_rec.person_uid);
        FETCH edw_general_extr.get_person_demo INTO pers_demo_rec;
        CLOSE edw_general_extr.get_person_demo;
--
        OPEN edw_general_extr.get_race(mif_cleanse_value, multi_src_rec.mtvparm_external_code, dedn_rec.person_uid);
        FETCH edw_general_extr.get_race INTO race_rec;
        CLOSE edw_general_extr.get_race;
--
        --BLM 3/25/15 changing from SYSDATE to eom_date_value to capture the value at the time rather than how it is today
        --OPEN edw_general_extr.get_visa(mif_cleanse_value, SYSDATE, multi_src_rec.mtvparm_external_code, employee_rec.person_uid);
        OPEN edw_general_extr.get_visa(mif_cleanse_value, eom_date_value, multi_src_rec.mtvparm_external_code, dedn_rec.person_uid);
        FETCH edw_general_extr.get_visa INTO visa_rec;
        CLOSE edw_general_extr.get_visa;
--
        --BLM 3/25/15 changing from SYSDATE to eom_date_value to capture the value at the time rather than how it is today
        --OPEN edw_general_extr.get_ra_visa(mif_cleanse_value, SYSDATE, multi_src_rec.mtvparm_external_code, employee_rec.person_uid);
        OPEN edw_general_extr.get_ra_visa(mif_cleanse_value, eom_date_value, multi_src_rec.mtvparm_external_code, dedn_rec.person_uid);
        FETCH edw_general_extr.get_ra_visa INTO visa_ra_rec;
        CLOSE edw_general_extr.get_ra_visa;
--
        OPEN edw_general_extr.get_vettype(mif_cleanse_value, multi_src_rec.mtvparm_external_code, dedn_rec.person_uid);
        FETCH edw_general_extr.get_vettype INTO vettype_rec;
        CLOSE edw_general_extr.get_vettype;
        
        OPEN get_sal_rate(multi_src_rec.mtvparm_external_code, dedn_rec.assign_salary_group, dedn_rec.assign_table, dedn_rec.assign_grade);
        FETCH get_sal_rate INTO sal_rate_rec;
        CLOSE get_sal_rate;
--
        OPEN get_faculty(multi_src_rec.mtvparm_external_code, dedn_rec.person_uid);
        FETCH get_faculty INTO faculty_rec;
        CLOSE get_faculty;
--
/**** Populate Dimensions ******/
       -- multi source
        ret_row.multi_source := dedn_rec.multi_source;
        ret_row.process_group := dedn_rec.zone_value;
        ret_row.administrative_group := dedn_rec.domain_value;
        ret_row.msrc_user_attribute_01 := ret_row.multi_source;
        ret_row.msrc_user_attribute_02 := NULL;
        ret_row.msrc_user_attribute_03 := NULL;
        ret_row.msrc_user_attribute_04 := NULL;
        ret_row.msrc_user_attribute_05 := NULL;
               
        -- time
        ret_row.event_qualifier := '*';
        ret_row.event := event_in;
        ret_row.event_date := event_date_value;
        ret_row.aid_year := NULL;
        ret_row.aid_period := NULL;
        ret_row.academic_year := NULL;
        ret_row.academic_period := NULL;
        ret_row.sub_academic_period := NULL;
        ret_row.fiscal_year := NULL;
        ret_row.fiscal_quarter := NULL;
        ret_row.fiscal_period := NULL;
        ret_row.calendar_year := calendar_year_in;
        ret_row.calendar_month := calendar_month_in;
        ret_row.time_user_attribute_01 := ret_row.multi_source;
        ret_row.time_user_attribute_02 := NULL;
        ret_row.time_user_attribute_03 := NULL;
        ret_row.time_user_attribute_04 := NULL;
        ret_row.time_user_attribute_05 := NULL;
        
        -- demographic
        ret_row.gender := pers_demo_rec.gender;
        ret_row.ethnicity_category := pers_demo_rec.ethnicity_category;
        ret_row.hispanic_latino_ethn_ind := pers_demo_rec.hispanic_latino_ethnicity_ind;
        ret_row.asian_ind := race_rec.asian_ind;
        ret_row.native_amer_or_alaskan_ind := race_rec.native_american_or_alaskan_ind;
        ret_row.black_or_african_ind := race_rec.black_or_african_ind;
        ret_row.pacific_islander_ind := race_rec.pacific_islander_ind;
        ret_row.white_ind := race_rec.white_ind;
        IF race_rec.number_of_races = 0 and visa_ra_rec.resident_alien_visa_count > 0 THEN
           ret_row.non_resident_ind := 'Y';
        ELSE
           ret_row.non_resident_ind := 'N';
        END IF;
        IF NVL(pers_demo_rec.hispanic_latino_ethnicity_ind,'N') = 'N' AND
           race_rec.number_of_races = 0 AND
           visa_ra_rec.resident_alien_visa_count = 0 THEN
           ret_row.RACE_ETHNICITY_UNKNOWN_IND := 'Y';
        ELSE
           ret_row.RACE_ETHNICITY_UNKNOWN_IND := 'N';
        END IF;
        ret_row.two_or_more_ind := race_rec.two_or_more_ind;
        ret_row.race_ethnicity_confirm_ind := pers_demo_rec.race_ethnicity_confirm_ind;
        ret_row.minority_ind := race_rec.minority_ind;
        ret_row.ethnicity := pers_demo_rec.ethnicity;
        ret_row.deceased_ind := pers_demo_rec.deceased_ind;
        ret_row.citizenship_ind := pers_demo_rec.citizenship_ind;
        ret_row.citizenship_type := pers_demo_rec.citizenship_type;
        ret_row.visa_type := visa_rec.visa_type;
        ret_row.nation_of_citizenship := pers_demo_rec.nation_of_citizenship;
        ret_row.nation_of_birth := pers_demo_rec.nation_of_birth;
        ret_row.primary_disability := pers_demo_rec.primary_disability;
        ret_row.legacy := pers_demo_rec.legacy;
        ret_row.marital_status := pers_demo_rec.marital_status;
        ret_row.religion := pers_demo_rec.religion;
        ret_row.veteran_type := vettype_rec.veteran_type;
        ret_row.veteran_category := pers_demo_rec.veteran_category;
        ret_row.demo_user_attribute_01 := ret_row.multi_source;
        ret_row.demo_user_attribute_02 := NULL;
        ret_row.demo_user_attribute_03 := NULL;
        ret_row.demo_user_attribute_04 := NULL;
        ret_row.demo_user_attribute_05 := NULL;        
        
        -- employee
        IF faculty_rec.person_uid IS NULL THEN
          ret_row.faculty_staff_ind := 'N';
        ELSE
          ret_row.faculty_staff_ind := 'Y';
        END IF;
        ret_row.active_position_ind := dedn_rec.active_position_ind;
        ret_row.employee_status := dedn_rec.employee_status;
        ret_row.employee_class := dedn_rec.employee_class;
        ret_row.employee_eeo_skill := dedn_rec.eeo_skill;
        ret_row.employee_grouping := dedn_rec.employee_grouping;
        ret_row.employee_time_status := dedn_rec.employee_time_status;
        IF (TRUNC(eom_date_value) < dedn_rec.adjusted_service_date) THEN
            ret_row.years_of_service_range := TRUNC((eom_date_value - dedn_rec.original_hire_date) / 365.25);
        ELSE
            ret_row.years_of_service_range := TRUNC((eom_date_value - dedn_rec.adjusted_service_date) / 365.25);
        END IF;
        ret_row.annual_salary_range := dedn_rec.annual_salary;
        ret_row.tenure := faculty_rec.tenure;
        ret_row.faculty_member_category := faculty_rec.faculty_member_category;
        ret_row.empl_user_attribute_01 := ret_row.multi_source;
        ret_row.empl_user_attribute_02 := NULL;
        ret_row.empl_user_attribute_03 := NULL;
        ret_row.empl_user_attribute_04 := NULL;
        ret_row.empl_user_attribute_05 := NULL;   
        
        -- administration
        ret_row.employer_code := dedn_rec.employer_code;
        ret_row.home_organization := dedn_rec.home_organization;
        ret_row.home_organization_chart := dedn_rec.home_organization_chart;
        ret_row.campus := dedn_rec.campus;
        ret_row.college := dedn_rec.college;
        ret_row.division := dedn_rec.division;
        ret_row.admn_user_attribute_01 := ret_row.multi_source;
        ret_row.admn_user_attribute_02 := NULL;
        ret_row.admn_user_attribute_03 := NULL;
        ret_row.admn_user_attribute_04 := NULL;
        ret_row.admn_user_attribute_05 := NULL;     
        
        -- deduction
        --do I need this? I already populated it .... ??
        --ret_row.multi_source   := ret_row.multi_source
        ret_row.carrier   := dedn_rec.carrier;
        ret_row.coverage_option    := dedn_rec.coverage_option;
--still need to put in the dedn_type (health, life, etc.)
--

        --fact table
        ret_row.person_uid := dedn_rec.edw_pidm;
        ret_row.age := MGKFUNC.F_CALCULATE_AGE(eom_date_value, pers_demo_rec.birth_date, pers_demo_rec.deceased_date);
        IF (TRUNC(eom_date_value) < dedn_rec.adjusted_service_date) THEN
            ret_row.years_of_service := TRUNC((eom_date_value - dedn_rec.original_hire_date) / 365.25);
        ELSE
            ret_row.years_of_service := TRUNC((eom_date_value - dedn_rec.adjusted_service_date) / 365.25);
        END IF;
        ret_row.employer_deduction_amount := dedn_rec.employer_dedn_amt;
        ret_row.employee_deduction_amount := dedn_rec.employee_dedn_amt;
        ret_row.system_load_process       := 'DEDUCTION';
        
        /*** We should wrap this party up, this has been enough fun */
        PIPE ROW(ret_row);
--
        ret_row := ret_row_init;
        pers_demo_rec := pers_demo_rec_init;
        race_rec := race_rec_init;
        visa_rec := visa_rec_init;
        visa_ra_rec := visa_ra_rec_init;
        vettype_rec := vettype_rec_init;
        sal_rate_rec := sal_rate_rec_init;
        faculty_rec := faculty_rec_init;
--
      END LOOP; -- dedn_rec
    END LOOP; -- multi_src_rec
--
    RETURN;
  END f_get_employee_dedn;
/******************************************************************************/
END zdw_employee_dedn_extr;


--show errors