CREATE OR REPLACE PROCEDURE 
--
  --  Module    : OMNI_LOAD_PROC 
  --  Author    : CBro
  --  Created    : 13/10/2008 10:34:05
  --  Purpose    : To load CSV files into local XE database for developers sandboxes
  --
  --  Revision History
  --
  --
 OMNI_LOAD_PROC(pSchemaOwner IN VARCHAR2 := 'MUSICSTATION',
                pDBDirectory IN VARCHAR2 := 'ORALOAD') AUTHID CURRENT_USER AS

  -- Local variables here
  vLoadCount NUMBER;
  c          NUMBER;
  ex         BOOLEAN;
  flen       NUMBER;
  bsize      NUMBER;
  vMarker    VARCHAR2(5) := '';
  g_Tblname   varchar2(30) default NULL;

  -- Entire table creation script here
  extTableString       VARCHAR2(32767) := NULL;
  extColString         VARCHAR2(32767) := NULL;
  extAccessParamString VARCHAR2(32767) := NULL;

  -- This is for one column
  extOneColString         VARCHAR2(4000) := NULL;
  extOneAccessParamString VARCHAR2(4000) := NULL;
  vColTransformStr        VARCHAR2(4000) := NULL;
  vFinalColTransformStr   VARCHAR2(32767) := NULL;  
  vTableWithCLOB          VARCHAR2(35) := NULL;
  vTmpColumnName          VARCHAR2(100) := NULL;
  -- max Columns in table
  vMaxCol      NUMBER(10) := 0;
  vExtRowCount NUMBER(10) := 0;

  -- Ext Table Name
  vExtTableName VARCHAR2(30) := NULL;
  vOrderBy      VARCHAR2(1000) := NULL;

  -- Newline on Windows platform
  l_str         VARCHAR2(5) := chr(13) || chr(10);
  vExtExistsFlg NUMBER(1) := 0;
  vTableExists  NUMBER(1) := 0;
begin
-- Main Exception block
begin
  
  -- Enable Parallel DML
  execute immediate 'alter session enable parallel dml';

  -- Disable all Constraints for tables
  begin
    <<disable_constraints_loop>>
    FOR x IN (SELECT 'alter table ' || c.table_name ||
                     ' DISABLE CONSTRAINT ' || c.constraint_name || '' sql_string
                FROM user_constraints c, user_tables a, global_table_list gt
               WHERE a.table_name = c.table_name
                 AND a.table_name = gt.table_name
                 AND c.constraint_type = 'R') LOOP
      EXECUTE IMMEDIATE x.sql_string;
    END LOOP disable_constraints_loop;
  exception
    when others then
      dbms_output.put_line('Discarded Error: ' || SQLCODE || ': ' ||
                           substr(SQLERRM, 1, 100));
      null; -- Do nothing and continue with processing 
  end;

  -- Loop through all the tables in the PDM only!
  <<tables_loop>>
  for tbl in (select a.table_name
                from all_tables a, global_table_list gt
               where a.table_name = gt.table_name
                 and a.owner = pSchemaOwner
                 and a.table_name not like 'DR$%'
                 and a.table_name not like 'MLOG$%'
                 and a.table_name not in ('EVENT_OBJ')
               order by a.table_name) loop
    g_Tblname := tbl.table_name;
    vTableWithCLOB := null;
    -- Check to see if there is csv to import at all:1
    utl_file.fgetattr(pDBDirectory,
                      upper(tbl.table_name) || '.csv',
                      ex,
                      flen,
                      bsize);
  
    IF ex THEN
      vTableExists := 1;
    ELSE
      vTableExists := 0;
    END IF;
  
    if (vTableExists = 1) then
    
      -- Phase I : Do a describe on each column of the tables in question
      -- start to build the External Table String              
      -- Get the max number of columns in the table  
      select max(column_id)
        into vMaxCol
        from user_tab_columns
       where table_name = tbl.table_name;
      vExtTableName := upper('X_' ||
                             regexp_replace(tbl.table_name, '_', ''));
    
      extTableString   := 'CREATE TABLE ' || vExtTableName || ' (' || l_str;
      vColTransformStr            := null;
      vFinalColTransformStr       := null;
      -- Get the column definitions
      <<columns_loop>>
      for col_x in (select a.COLUMN_ID,
                           a.COLUMN_NAME,
                           a.DATA_TYPE,
                           a.DATA_PRECISION,
                           a.DATA_SCALE,
                           a.DATA_LENGTH,
                           a.DEFAULT_LENGTH,
                           a.CHARACTER_SET_NAME,
                           a.CHAR_LENGTH,
                           a.CHAR_USED
                      from user_tab_columns a
                     where table_name = tbl.table_name
                     order by a.column_id) loop
        -- If its a NUMBER datatype, then format accordingly
        if (upper(col_x.data_type) = 'NUMBER') then
          -- There exists a scale
          if (col_x.data_scale is not null AND col_x.data_scale > 0) then
            extOneColString := 'NUMBER(' || col_x.data_precision || ',' ||
                               col_x.data_scale || '),' || l_str;
          else
            extOneColString := 'NUMBER(' || col_x.data_precision || '),' ||
                               l_str;
          end if;
        
          -- Also build the access parameter string
          extOneAccessParamString := 'CHAR(' || col_x.data_length * 2 || '),' ||
                                     l_str;
        end if; -- End of Number Checking!
      
        -- If its a VARCHAR2 datatype, then format accordingly
        if (upper(col_x.data_type) = 'VARCHAR2') then
          if (upper(col_x.char_used) = 'C') then
            extOneColString := 'VARCHAR2(' || col_x.char_length ||
                               ' CHAR),' || l_str;
          else
            extOneColString := 'VARCHAR2(' || col_x.char_length || '),' ||
                               l_str;
          end if;
          -- Also build the access parameter string
          extOneAccessParamString := 'CHAR(' || col_x.data_length * 2 || '),' ||
                                     l_str;
        
        end if;
      
        -- If its a CHAR datatype, then format accordingly
        if (upper(col_x.data_type) = 'CHAR') then
          if (upper(col_x.char_used) = 'C') then
            extOneColString := 'CHAR(' || col_x.char_length || ' CHAR),' ||
                               l_str;
          else
            extOneColString := 'CHAR(' || col_x.char_length || '),' ||
                               l_str;
          end if;
          -- Also build the access parameter string
          extOneAccessParamString := 'CHAR(' || col_x.data_length * 2 || '),' ||
                                     l_str;
        end if;
      
        -- If its a FLOAT datatype, then format accordingly
        if (upper(col_x.data_type) = 'FLOAT') then
          -- There exists a scale
          if (col_x.data_scale is not null AND col_x.data_scale > 0) then
            extOneColString := 'FLOAT(' || col_x.data_precision || ',' ||
                               col_x.data_scale || '),' || l_str;
          else
            extOneColString := 'FLOAT(' || col_x.data_precision || '),' ||
                               l_str;
          end if;
          -- Also build the access parameter string
          extOneAccessParamString := 'CHAR(' || col_x.data_length * 2 || '),' ||
                                     l_str;
        end if;
      
        -- If its a TIMESTAMP(3) datatype, then format accordingly
        if (upper(col_x.data_type) like 'TIMESTAMP%') then
          extOneColString         := col_x.data_type || ',' || l_str;
          extOneAccessParamString := 'char date_format timestamp mask "DD-MON-YYYY HH24.MI.SSXFF", ' ||
                                     l_str;
        end if;
      
        -- If its a DATE or CLOB leave it alone
        if (upper(col_x.data_type) = 'DATE' or
           upper(col_x.data_type) = 'CLOB') then
          extOneColString := col_x.data_type || ',' || l_str;
        
          if (upper(col_x.data_type) = 'CLOB') then
            extOneAccessParamString := 'CHAR(200),' || l_str; -- Its char(200) because it only stored the placeholder for external filename
            vTableWithCLOB          := tbl.table_name;
          else
            extOneAccessParamString := 'DATE ''DD-MON-YYYY HH24.MI.SS'', ' ||
                                       l_str;
          end if;
        end if;
      
        -- After all the Checks done for data types now its time to append to the main string buffer
        -- Last column, trim off the , at the end
        if (vMaxCol = col_x.column_id) then
          extOneColString         := regexp_replace(extOneColString,
                                                    ',',
                                                    '');
          extOneAccessParamString := regexp_replace(extOneAccessParamString,
                                                    ',',
                                                    '');
        
        end if;
        extColString := extColString || '"' || col_x.column_name || '"  ' ||
                        extOneColString;
      
        -- If its a CLOB we have to do a Columns Transform Operation
        if (upper(col_x.data_type) = 'CLOB') then
          vTmpColumnName   := col_x.column_name || '_REF';
          vColTransformStr := col_x.column_name || ' FROM LOBFILE (' ||
                              vTmpColumnName || ') FROM (' || pDBDirectory ||
                              ') CLOB,' ||l_str;
        else
          vTmpColumnName := col_x.column_name;
        end if;
        extAccessParamString    := extAccessParamString || '"' ||
                                   vTmpColumnName || '"  ' ||
                                   extOneAccessParamString;
        extOneColString         := null; -- Null out the string
        extOneAccessParamString := null; -- Null out the string
      
        -- This will get rid of the comma from the very last column string attached !
        if (vMaxCol = col_x.column_id) then
          vColTransformStr := regexp_replace(vColTransformStr, ',', '');
        end if;
        -- Store the string
        vFinalColTransformStr := vFinalColTransformStr || vColTransformStr;

        -- End of looping through columns
      end loop columns_loop;
    
      -- Now append to the main table creation string
      extTableString := extTableString || extColString || ')' || l_str;
      extColString   := null; -- Null out the string
    
      -- Add the Organization, access parameters bit!
      extTableString := extTableString ||
                        'organization external ( type oracle_loader ' ||
                        l_str || ' default directory ' || pDBDirectory ||
                        ' access parameters (' || l_str;
      extTableString := extTableString ||
                        'RECORDS DELIMITED BY 0x''5E5E5E5E0D0A'' CHARACTERSET AL32UTF8 BADFILE ''x_' ||
                        tbl.table_name || '.bad''' || l_str;
      extTableString := extTableString || 'LOGFILE ''x_' || tbl.table_name ||
                        '.log''' || l_str;
      extTableString := extTableString ||
                        'READSIZE 1048576 SKIP 1 FIELDS TERMINATED BY 0x''2C'' OPTIONALLY ENCLOSED BY 0x''7C7C'' LDRTRIM MISSING FIELD VALUES ARE NULL ' ||
                        l_str;
      extTableString := extTableString ||
                        'REJECT ROWS WITH ALL NULL FIELDS ( ' || l_str;
    
      -- Add the extAccessParamString here
      if (vColTransformStr is null) then
        -- There was no CLOBs in this table!
        extTableString := extTableString || extAccessParamString || ')' ||
                          l_str;
      else
        extTableString := extTableString || extAccessParamString || ')' ||
                          'COLUMN TRANSFORMS (' || vFinalColTransformStr || ')' ||
                          l_str;
      end if;
    
      extAccessParamString := null;
      extTableString       := extTableString || ') location (''' ||
                              upper(tbl.table_name) ||
                              '.csv'') ) reject limit unlimited parallel nomonitoring ';
      vColTransformStr     := null;
    
      -- DEBUG ONLY : dbms_output.put_line(extTableString);
    
      -- Check to see if this table already exists, if it does go ahead and drop external table.
      -- Exception Block
      begin
      
        select 1
          into vExtExistsFlg
          from user_tables
         where upper(table_name) = vExtTableName;
        if (vExtExistsFlg = 1) then
          execute immediate 'drop table ' || vExtTableName;
        end if;
      
      exception
        when no_data_found then
          --dbms_output.put_line('Discarded Error: '|| SQLCODE ||': ' ||substr(SQLERRM, 1, 100) );
          null; -- Do nothing and continue with processing 
      end;
      -- Creates the external table from scratch
      execute immediate extTableString;
    
      -- Check to see what the count of rows in the external table to load data from is and display
      execute immediate 'select count(*) from ' || vExtTableName
        into vLoadCount;
    
      -- Check if this count is zero, then there is no need to load the table's data
      if (vLoadCount = 0) then
        null;
      else
        -- Check to see if the external table is the same as target table
        if (tbl.table_name = vTableWithCLOB) then
          vExtRowCount := 1;
        else
          execute immediate 'select count(*) from (( select * from ' ||
                            tbl.table_name || ' minus select * from ' ||
                            vExtTableName ||
                            ' ) union all ( select * from ' ||
                            vExtTableName || ' minus select * from ' ||
                            tbl.table_name || ' ))'
            into vExtRowCount;
        end if;
      
        -- Check to see if there are any changes and only then go ahead and refresh the DB
        -- with the latest changes in the .csv file!    
        vMarker := '';
        if (vExtRowCount > 0) then
        
          -- At this point the External Table is filled up
          --dbms_output.put_line('truncating -->'|| tbl.table_name);
          execute immediate 'truncate table ' || tbl.table_name;
        
          vOrderBy := omni_unload_pkg.find_pk(p_tname => tbl.table_name);
          if (vOrderBy is NULL) then
            -- Fill up the target table with the external table's content using DIRECT PATH INSERT Statement
            execute immediate 'insert /*+ append nologging */ into ' ||
                              tbl.table_name || ' select * from ' ||
                              vExtTableName;
          else
            -- Fill up the target table with the external table's content using DIRECT PATH INSERT Statement
            execute immediate 'insert /*+ append nologging */ into ' ||
                              tbl.table_name || ' select * from ' ||
                              vExtTableName || ' order by ' || vOrderBy;
          end if;
          vMarker := '(++)';
        end if;
      
      end if;
    
      -- Drop the external table as it is not needed anymore.
      execute immediate 'drop table ' || vExtTableName;
      vExtRowCount := 0;
    
      -- Display what got loaded
      dbms_output.put_line(vMarker ||
                           ' Loaded from CSV file into Table Name = ' ||
                           tbl.table_name);
    
    end if; -- End of if file exists in the table
  
  -- End the table loop
  end loop tables_loop;

  -- Commit all the modifications
  execute immediate 'commit';

  -- Enable all Constraints for tables
  begin
    <<enable_constraints_loop>>
    FOR x IN (SELECT 'alter table ' || c.table_name || ' ENABLE CONSTRAINT ' ||
                     c.constraint_name || '' sql_string
                FROM user_constraints c, user_tables a, global_table_list gt
               WHERE a.table_name = c.table_name
                 AND a.table_name = gt.table_name
                 AND c.constraint_type = 'R') LOOP
      EXECUTE IMMEDIATE x.sql_string;
    END LOOP enable_constraints_loop;
  exception
    when others then
      dbms_output.put_line('Discarded Error: ' || SQLCODE || ': ' ||
                           substr(SQLERRM, 1, 100));
      null; -- Do nothing and continue with processing 
  end;

  -- Disable Parallel DML
  execute immediate 'alter session disable parallel dml';

exception when others then       
        dbms_output.put_line('******* (ERROR) : Unexpected Error while processing table ' ||
                             g_Tblname || ': ' || SQLCODE || ': ' ||
                             substr(SQLERRM, 1, 200));
        
  -- Re-Enable all Constraints for tables whenever you have 
  -- to break out of processing because of some error encountered.
  begin
    <<enable_constraints_loop>>
    FOR x IN (SELECT 'alter table ' || c.table_name || ' ENABLE CONSTRAINT ' ||
                     c.constraint_name || '' sql_string
                FROM user_constraints c, user_tables a, global_table_list gt
               WHERE a.table_name = c.table_name
                 AND a.table_name = gt.table_name
                 AND c.constraint_type = 'R') LOOP
      EXECUTE IMMEDIATE x.sql_string;
    END LOOP enable_constraints_loop;
  exception
    when others then
      dbms_output.put_line('Discarded Error: ' || SQLCODE || ': ' ||
                           substr(SQLERRM, 1, 200));
      null; -- Do nothing and continue with processing 
  end;
  
  raise;                     
end;                             
end OMNI_LOAD_PROC;
/
