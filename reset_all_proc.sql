CREATE OR REPLACE PROCEDURE
--
  --  Module    : RESET_ALL_PROC 
  --  Author    : Manu Kaul
  --  Created    : 13/10/2008 10:34:05
  --  Purpose    : To reset all sequences
  --
  --  Revision History
  --
  --  Date         Who              Version          Details
  --  ===========  ===============  ===============  =======================================
  --  13/10/2008   Manu Kaul        V1.0.MKA.0.0     Initial Version
  --
 RESET_ALL_PROC AS

 vTableName VARCHAR2(30) := null;
 vBindTbl   VARCHAR2(30) := null;
begin
       -- Loop through every sequence resetting it
       for seq in (select upper(seq_name) seq_name from global_seq_list order by seq_name) loop
           -- test to see if table exists or not
           begin
              vBindTbl := regexp_replace(seq.seq_name,'_ID_SEQ','');
              
              select table_name 
              into   vTableName 
              from   user_tables
              where  table_name = vBindTbl;
                            
           -- Resets each individual sequence
           reset_sequence_proc( seq.seq_name, vTableName, 'ID');
           
          exception  
              when no_Data_found then 
              dbms_output.put_line('No table found for -->'|| vBindTbl);
              null;
           end;
           

       end loop;  

end Reset_All_proc;
/