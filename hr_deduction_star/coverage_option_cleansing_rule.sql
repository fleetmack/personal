select distinct 'COVERAGE_OPTION_C' DATA_ELEMENT,
MIF_VALUE W_PREFIX,
coverage_option W_VALUE, 
'No Desc' long_desc,
'No Desc' short_desc
from mpt_benefit_deduct
where coverage_option is not null
union all
select distinct 'COVERAGE_OPTION_C' DATA_ELEMENT,
MIF_VALUE W_PREFIX,
'None' W_VALUE, 
'No Desc' long_desc,
'No Desc' short_desc
from mpt_benefit_deduct
