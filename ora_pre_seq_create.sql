--
-- SQL Plus commands 
--
SET FEEDBACK OFF
SET VERIFY OFF



-- Create a Global list of sequences
WHENEVER SQLERROR EXIT SQL.SQLCODE
declare
  vFound integer :=0;
begin
  begin -- Exception Block
  
  for x in (select t.table_name from user_tables t where t.table_name = 'GLOBAL_SEQ_LIST' ) loop
	vFound := 1;  
  end loop; 
  
  -- If it was found then it already exists.
  if( vFound = 0 ) then
	execute immediate 'create table global_seq_list ( seq_name varchar2(50) default NULL )';
  end if;
  
  exception
        when others then
             vFound :=0;
	     raise; 
  end;   
end;
/

-- Break here to get the list of tables from filesystem 



EXIT SUCCESS
