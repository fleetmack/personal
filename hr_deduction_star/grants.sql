Need these grants for all tables (input, clean, wkeys, dim, fact) in order for your mappings to work:

grant ALTER	on ztt_employee_deduction_clean to IA_ADMIN;
grant DELETE	on ztt_employee_deduction_clean to IA_ADMIN;
grant INDEX	on ztt_employee_deduction_clean to IA_ADMIN;
grant INSERT	on ztt_employee_deduction_clean to IA_ADMIN;
grant SELECT	on ztt_employee_deduction_clean to EDWMGR;
grant SELECT	on ztt_employee_deduction_clean to IA_ADMIN;
grant UPDATE	on ztt_employee_deduction_clean to IA_ADMIN;
grant REFERENCES	on ztt_employee_deduction_clean to IA_ADMIN;
grant ON COMMIT REFRESH	on ztt_employee_deduction_clean to IA_ADMIN;
grant QUERY REWRITE	on ztt_employee_deduction_clean to IA_ADMIN;
grant DEBUG	on ztt_employee_deduction_clean to IA_ADMIN;
grant FLASHBACK	on ztt_employee_deduction_clean to IA_ADMIN;
