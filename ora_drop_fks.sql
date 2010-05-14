SET FEEDBACK OFF
SET VERIFY OFF

-- At this point I have all the list of tables in my global table 
-- I also know the table generation scripts to run!
-- Check to see if the index exists and then drop the user
prompt Verifying FK existence and started DROP process ;
WHENEVER SQLERROR EXIT SQL.SQLCODE
declare
  i integer;
begin
  begin -- Exception Block
  for x in ( 
		select uc.constraint_name,
		      uc.table_name 
		from  user_constraints uc,
		      global_table_list t
		where uc.table_name = t.table_name
		and   uc.constraint_type = 'R'
		and   uc.owner = '&&1') 
  loop
      execute immediate 'ALTER TABLE '|| x.table_name ||' DROP CONSTRAINT '||x.constraint_name ;
  end loop; 
  exception
        when others then
	     raise; 
  end;   
end;
/

prompt Drop FK process completed [OK]

