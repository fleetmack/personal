/* This function takes in a listagg value and a delimiter, removes duplicate values, and returns the result set */
/* Example: The listagg function as such:
     listagg(ia.mif_value,',') within GROUP (ORDER BY  ia.person_uid, ia.academic_period)
     	This returns the following: CCA,CCA,CCA,CCD,CCD,CCD,CCD
 	
 	Example using my function:
    distinct_listagg( listagg(ia.mif_value,',') within GROUP (ORDER BY ia.person_uid, ia.academic_period),',')	
    	This returns the following: CCA,CCD

  */



create or replace function distinct_listagg
  (listagg_in varchar2,
   delimiter_in varchar2)
   
   return varchar2
   as
   hold_result varchar2(4000);
   begin
   
   select rtrim( regexp_replace( (listagg_in)
          , '([^'||delimiter_in||']*)('||
          delimiter_in||'\1)+($|'||delimiter_in||')', '\1\3'), ',')
          into hold_result
          from dual;
          
    return hold_result;
    
end;