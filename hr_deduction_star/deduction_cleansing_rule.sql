SELECT 'DEDUCTION_C' DATA_ELEMENT,
  p1.ptrbdca_vpdi_code W_PREFIX,
  p1.ptrbdca_code W_VALUE,
  nvl((select p2.ptrbdca_long_desc from ptrbdca p2 where p2.ptrbdca_vpdi_code = 'CCCS' and p1.ptrbdca_code = p2.ptrbdca_code),'No CCCS Desc') LONG_DESC,
 nvl((select p2.ptrbdca_short_desc from ptrbdca p2 where p2.ptrbdca_vpdi_code = 'CCCS' and p1.ptrbdca_code = p2.ptrbdca_code),'No CCCS Desc') SHORT_DESC
 FROM ptrbdca p1