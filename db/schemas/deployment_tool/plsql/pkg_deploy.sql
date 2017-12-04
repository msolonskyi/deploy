create or replace package pkg_deploy is

  -- Author  : Mykola Solonskyi
  -- Created : 08/10/2014 12:21:04
  -- Purpose : Autopatic deploy table from XML structure

---------------------------------------
procedure sp_deploy_table_by_xml_struct(p_xml clob);

end pkg_deploy;
/

create or replace package body pkg_deploy is

CRLF           constant char(2)       := chr(13) || chr(10);
cv_module_name constant varchar2(256) := 'deploy tables structure';
cb_is_output   constant boolean       := true;
---------------------------------------
procedure sp_deploy_table_by_xml_struct(p_xml clob)
as
  vt_xml_columns_table      t_columns_table;
  vt_db_columns_table       t_columns_table;
  vt_add_columns_table      t_columns_table := t_columns_table();
  vt_modify_columns_table   t_columns_table := t_columns_table();
  vt_mod_col_def_val_table  t_columns_table := t_columns_table();
  vt_drop_columns_table     t_columns_table := t_columns_table();
  --
  vt_xml_constraints_table  t_constraints_table;
  vt_db_constraints_table   t_constraints_table;
  vt_add_constraints_table  t_constraints_table;
  vt_mod_constraints_table  t_constraints_table;
  vt_drop_constraints_table t_constraints_table;

  vt_xml_indexes_table      t_indexes_table;
  vt_db_indexes_table       t_indexes_table;
  vt_mod_indexes_table      t_indexes_table;
  vt_add_indexes_table      t_indexes_table := t_indexes_table();
  vt_drop_indexes_table     t_indexes_table := t_indexes_table();
  --
  vt_columns_pair_table     t_columns_pair_table;
  vv_table_owner            varchar2(30);
  vv_table_name             varchar2(30);
  vv_table_type             varchar2(30);
  vv_table_comments         varchar2(4000);
  vn_qty                    number;
  vc_sql                    clob := empty_clob();
  vc_add_columns_sql        clob := empty_clob();
  vc_modify_columns_sql     clob := empty_clob();
  vc_drop_columns_sql       clob := empty_clob();
  vc_mod_col_def_val_sql    clob := empty_clob();
  i                         pls_integer;
  vv_stage                  varchar2(4000);
begin
  vv_stage := 'start';
  pkg_log.sp_log_message(pv_module => cv_module_name, pv_text => vv_stage, pb_is_output => cb_is_output);
  --table name
  select nvl(upper(trim(owner)), user) as owner, upper(trim(name)) as name, upper(trim(type)) as type, trim(comments) as comments
  into vv_table_owner, vv_table_name, vv_table_type, vv_table_comments
  from xmltable('/table'
                 passing xmltype(p_xml)
                 columns
                    owner    varchar2(30)   path '@owner',
                    name     varchar2(30)   path '@name',
                    type     varchar2(30)   path '@type',
                    comments varchar2(4000) path '@comments') xml;
  -- columns list
  select t_column(name, type, char_length, data_precision, data_scale, char_used, nullable, default_value, virtual_expression, comments)
  bulk collect into vt_xml_columns_table
  from (select name,
               type,
               case
                 when type in ('VARCHAR2', 'VARCHAR', 'CHAR') then to_number(trim(regexp_substr(n, '^(\d+)\s*(\,?\s*(\d+))?(\w+)?\s*', 1, 1, 'i', 1)))
                 else 0
               end char_length,
               case
                 when type in ('NUMBER', 'TIMESTAMP') then to_number(trim(regexp_substr(n, '^(\d+)\s*(\,?\s*(\d+))?(\w+)?\s*', 1, 1, 'i', 1)))
               end data_precision,
               decode(to_number(trim(regexp_substr(n, '^(\d+)\s*(\,?\s*(\d+))?(\w+)?\s*', 1, 1, 'i', 3))), 0, null) data_scale,
               pkg_utils.sf_normalize_char_used(
                 case
                   when type in ('VARCHAR2', 'VARCHAR', 'CHAR') then nvl(regexp_substr(n, '^(\d+)\s*(\,?\s*(\d+))?(\w+)?\s*', 1, 1, 'i', 4), 'B')
                   else regexp_substr(n, '^(\d+)\s*(\,?\s*(\d+))?(\w+)?\s*', 1, 1, 'i', 4)
                 end ) char_used,
               nullable,
               default_value,
               virtual_expression,
               comments
        from (select upper(trim(name)) as name,
                             upper(trim(regexp_substr(type, '^\s*(\w+)\s*(\(\s*(.*)\s*\))?', 1, 1, 'i', 1))) type,
                             upper(trim(regexp_substr(type, '^\s*(\w+)\s*(\(\s*(.*)\s*\))?', 1, 1, 'i', 3))) n,
                             upper(nvl(trim(nullable), 'y')) as nullable,
                             upper(trim(default_value)) as default_value,
                             upper(trim(virtual_expression)) as virtual_expression,
                             comments
                      from xmltable('/table/columns/column'
                                     passing xmltype(p_xml)
                                     columns
                                        name               varchar2(30)   path '@name',
                                        type               varchar2(30)   path '@type',
                                        nullable           char(1)        path '@nullable',
                                        default_value      varchar2(30)   path '@default_value',
                                        virtual_expression varchar2(4000) path '@virtual_expression',
                                        comments           varchar2(4000) path '@comments') xml));
  -- constraints list
  select t_constraint(name, type, columns_list, foreign_owner, foreign_table, foreign_columns_list, condition, delete_rule, status, validated)
  bulk collect into vt_xml_constraints_table
  from (select upper(trim(name)) as name,
               upper(trim(type)) as type,
               upper(replace(columns_list, ' ', '')) as columns_list,
               case
                 when upper(trim(type)) = 'FOREIGN KEY' then upper(nvl(trim(foreign_owner), user))
                 else upper(trim(foreign_owner))
               end as foreign_owner,
               upper(trim(foreign_table)) as foreign_table,
               upper(replace(foreign_columns_list, ' ', '')) as foreign_columns_list,
               upper(trim(condition)) as condition,
               case
                 when upper(trim(type)) = 'FOREIGN KEY' then upper(nvl(trim(delete_rule), 'NO ACTION'))
                 else upper(trim(delete_rule))
               end as delete_rule,
               case
                 when upper(trim(status)) in ('ENABLED', 'ENABLE') then 'ENABLE'
                 when upper(trim(status)) is null then 'ENABLE'
                 when upper(trim(status)) in ('DISABLED', 'DISABLE') then 'DISABLE'
                 else null
               end as status,
               case
                 when upper(trim(validated)) in ('VALIDATED', 'VALIDATE') then 'VALIDATE'
                 when upper(trim(validated)) is null then 'VALIDATE'
                 when upper(trim(validated)) in ('NOT VALIDATED', 'NOVALIDATE') then 'NOVALIDATE'
                 else null
               end as validated
        from xmltable('/table/constraints/constraint'
                       passing xmltype(p_xml)
                       columns
                          name                 varchar2(30)   path '@name',
                          type                 varchar2(30)   path '@type',
                          columns_list         varchar2(4000) path '@columns_list',
                          foreign_owner        varchar2(30)   path '@foreign_owner',
                          foreign_table        varchar2(30)   path '@foreign_table',
                          foreign_columns_list varchar2(4000) path '@foreign_columns_list',
                          delete_rule          varchar2(9)    path '@delete_rule',
                          condition            varchar2(4000) path '@condition',
                          status               varchar2(4000) path '@status',
                          validated            varchar2(4000) path '@validated') xml);
  -- indexes list
  select t_index(name, type, uniqueness, clause, visibility)
  bulk collect into vt_xml_indexes_table
  from (select upper(trim(name)) as name,
               case
                 when upper(trim(type)) = 'BITMAP' then upper(trim(type))
                 else null
               end as type,
               case
                 when upper(trim(uniqueness)) = 'UNIQUE' then upper(trim(uniqueness))
                 else null
               end as uniqueness,
               replace(upper(trim(clause)), ' ', '') as clause,
               case
                 when upper(trim(visibility)) = 'INVISIBLE' then upper(trim(visibility))
                 else 'VISIBLE'
               end as visibility
        from xmltable('/table/indexes/index'
                       passing xmltype(p_xml)
                       columns
                          name       varchar2(30)   path '@name',
                          type       varchar2(30)   path '@type',
                          uniqueness varchar2(30)   path '@uniqueness',
                          clause     varchar2(4000) path '@clause',
                          visibility varchar2(9)    path '@visibility') xml);
  -- partitions list

  select count(1)
  into vn_qty
  from all_tables
  where owner = vv_table_owner
    and table_name = vv_table_name;
  -- 1. create table
  if vn_qty = 0 then
    vc_sql := 'create table ' || vv_table_owner || '.' || vv_table_name || '(';
    -- 1.1. columns
    vv_stage := '1.1. columns';
    i := vt_xml_columns_table.first;
    while i is not null
    loop
      vc_sql := vc_sql || vt_xml_columns_table(i).mf_get_add_column_string || ',';
      i := vt_xml_columns_table.next(i);
    end loop;
    --
    vc_sql := trim(trim(both ',' from vc_sql)) || ')';
    --
    pkg_log.sp_log_message(pv_module => cv_module_name, pv_text => vv_stage, pv_clob => vc_sql, pb_is_output => cb_is_output);
    execute immediate vc_sql;
    --
    -- 1.2. comments
    -- 1.2.1. table comments
    vv_stage := '1.2.1. table comments';
    if vv_table_comments is not null then
      vc_sql := 'comment on table ' || vv_table_owner || '.' || vv_table_name || ' is ''' || vv_table_comments || '''';
      --
      pkg_log.sp_log_message(pv_module => cv_module_name, pv_text => vv_stage, pv_clob => vc_sql, pb_is_output => cb_is_output);
      execute immediate vc_sql;
    end if;
    --
    -- 1.2.2. columns comments
    vv_stage := '1.2.2. columns comments';
    i := vt_xml_columns_table.first;
    while i is not null
    loop
      if (vt_xml_columns_table(i).comments is not null) then
        vc_sql := 'comment on column ' || vv_table_owner || '.' || vv_table_name || '.' || vt_xml_columns_table(i).name || ' is ''' || vt_xml_columns_table(i).comments || '''';
        --
        pkg_log.sp_log_message(pv_module => cv_module_name, pv_text => vv_stage, pv_clob => vc_sql, pb_is_output => cb_is_output);
        execute immediate vc_sql;
      end if;
      i := vt_xml_columns_table.next(i);
    end loop;
    --
    -- 1.3. constraints
    vv_stage := '1.3. constraints';
    i := vt_xml_constraints_table.first;
    while i is not null
    loop
      vc_sql := 'alter table ' || vv_table_owner || '.' || vv_table_name || vt_xml_constraints_table(i).mf_get_create_string;
      --
      pkg_log.sp_log_message(pv_module => cv_module_name, pv_text => vv_stage, pv_clob => vc_sql, pb_is_output => cb_is_output);
      execute immediate vc_sql;
      --
      i := vt_xml_constraints_table.next(i);
    end loop;
    --
    -- 1.4. indexes
    vv_stage := '1.4. indexes';
    i := vt_xml_indexes_table.first;
    while i is not null
    loop
      vc_sql := 'create ' || vt_xml_indexes_table(i).type || ' index ' || vt_xml_indexes_table(i).name || ' on ' || vv_table_name || ' (' || vt_xml_indexes_table(i).clause || ')';
      --
      pkg_log.sp_log_message(pv_module => cv_module_name, pv_text => vv_stage, pv_clob => vc_sql, pb_is_output => cb_is_output);
      execute immediate vc_sql;
      --
      i := vt_xml_indexes_table.next(i);
    end loop;
  -- 2. modify structure
  else
    -- 2.1. columns
    select t_column(name, type, char_length, data_precision, data_scale, char_used, nullable, default_value, virtual_expression, comments)
    bulk collect into vt_db_columns_table
    from (select tc.column_name as name,
                 case
                   when tc.virtual_column = 'YES' then null
                   else data_type
                 end as type,
                 tc.char_length,
                 tc.data_precision,
                 decode(tc.data_scale, 0, null) data_scale,
                 pkg_utils.sf_normalize_char_used(tc.char_used) char_used,
                 tc.nullable,
                 case
                   when pkg_utils.sf_get_data_default(tc.owner, tc.table_name, tc.column_name) is not null and
                        upper(trim(pkg_utils.sf_get_data_default(tc.owner, tc.table_name, tc.column_name))) != 'NULL' and
                        (tc.virtual_column = 'NO') then
                            pkg_utils.sf_get_data_default(tc.owner, tc.table_name, tc.column_name)
                   else null
                 end default_value,
                 case
                     when pkg_utils.sf_get_data_default(tc.owner, tc.table_name, tc.column_name) is not null and tc.virtual_column = 'YES' then pkg_utils.sf_get_data_default(tc.owner, tc.table_name, tc.column_name)
                     else null
                 end virtual_expression,
                 cc.comments
          from all_tab_cols tc, all_col_comments cc
          where tc.owner       = cc.owner(+)
            and tc.table_name  = cc.table_name(+)
            and tc.column_name = cc.column_name(+)
            and tc.owner       = vv_table_owner
            and tc.table_name  = vv_table_name
            and tc.column_id is not null);
    --
    select t_columns_pair(t_column(db.name, db.type, db.char_length, db.data_precision, db.data_scale, db.char_used, db.nullable, db.default_value, db.virtual_expression, db.comments),
                          t_column(xml.name, xml.type, xml.char_length, xml.data_precision, xml.data_scale, xml.char_used, xml.nullable, xml.default_value, xml.virtual_expression, xml.comments))
    bulk collect into vt_columns_pair_table
    from table(vt_xml_columns_table) xml full join table(vt_db_columns_table) db on (xml.name = db.name);
    --
    i := vt_columns_pair_table.first;
    while i is not null
    loop
      -- 2.1.1. add columns
      if (vt_columns_pair_table(i).db_column.name is null) then
        vt_add_columns_table.extend;
        vt_add_columns_table(vt_add_columns_table.last) := t_column(vt_columns_pair_table(i).xml_column.name,
                                                                    vt_columns_pair_table(i).xml_column.type,
                                                                    vt_columns_pair_table(i).xml_column.char_length,
                                                                    vt_columns_pair_table(i).xml_column.data_precision,
                                                                    vt_columns_pair_table(i).xml_column.data_scale,
                                                                    vt_columns_pair_table(i).xml_column.char_used,
                                                                    vt_columns_pair_table(i).xml_column.nullable,
                                                                    vt_columns_pair_table(i).xml_column.default_value,
                                                                    vt_columns_pair_table(i).xml_column.virtual_expression,
                                                                    vt_columns_pair_table(i).xml_column.comments);
      -- 2.1.2. modify column types
      elsif ((vt_columns_pair_table(i).db_column.name = vt_columns_pair_table(i).xml_column.name) 
         and (vt_columns_pair_table(i).db_column.mf_equals_without_default_val != vt_columns_pair_table(i).xml_column.mf_equals_without_default_val)) then
        --
        vt_modify_columns_table.extend;
        -- 2.1.2.1 initialization + name
        vt_modify_columns_table(vt_modify_columns_table.last) := t_column(vt_columns_pair_table(i).xml_column.name,
                                                                          null, null, null, null, null, null, null, null, null);
        -- 2.1.2.2 type != type
        if ((vt_columns_pair_table(i).db_column.type != vt_columns_pair_table(i).xml_column.type) and
            (vt_columns_pair_table(i).db_column.type is not null) and
            (vt_columns_pair_table(i).xml_column.type is not null)) then
          vt_modify_columns_table(vt_modify_columns_table.last).type           := vt_columns_pair_table(i).xml_column.type;
          vt_modify_columns_table(vt_modify_columns_table.last).char_length    := vt_columns_pair_table(i).xml_column.char_length;
          vt_modify_columns_table(vt_modify_columns_table.last).data_precision := vt_columns_pair_table(i).xml_column.data_precision;
          vt_modify_columns_table(vt_modify_columns_table.last).data_scale     := vt_columns_pair_table(i).xml_column.data_scale;
          vt_modify_columns_table(vt_modify_columns_table.last).char_used      := vt_columns_pair_table(i).xml_column.char_used;
        end if;
        -- 2.1.2.3
        if vt_columns_pair_table(i).db_column.mf_type_to_string != vt_columns_pair_table(i).xml_column.mf_type_to_string then
          vt_modify_columns_table(vt_modify_columns_table.last).type           := vt_columns_pair_table(i).xml_column.type;
          vt_modify_columns_table(vt_modify_columns_table.last).char_length    := vt_columns_pair_table(i).xml_column.char_length;
          vt_modify_columns_table(vt_modify_columns_table.last).data_precision := vt_columns_pair_table(i).xml_column.data_precision;
          vt_modify_columns_table(vt_modify_columns_table.last).data_scale     := vt_columns_pair_table(i).xml_column.data_scale;
          vt_modify_columns_table(vt_modify_columns_table.last).char_used      := vt_columns_pair_table(i).xml_column.char_used;
        end if;
        -- 2.1.2.3 type + virtual_expression
        if not ((vt_columns_pair_table(i).db_column.virtual_expression = vt_columns_pair_table(i).xml_column.virtual_expression) or
                (vt_columns_pair_table(i).db_column.virtual_expression is null and vt_columns_pair_table(i).xml_column.virtual_expression is null)) then
          vt_modify_columns_table(vt_modify_columns_table.last).virtual_expression       := vt_columns_pair_table(i).xml_column.virtual_expression;
        end if;
        -- 2.1.2.4 nullable
        if (vt_columns_pair_table(i).db_column.nullable != vt_columns_pair_table(i).xml_column.nullable) then
          vt_modify_columns_table(vt_modify_columns_table.last).nullable       := vt_columns_pair_table(i).xml_column.nullable;
        end if;
      elsif ((vt_columns_pair_table(i).db_column.name = vt_columns_pair_table(i).xml_column.name) 
         and (vt_columns_pair_table(i).db_column.default_value || '_' != vt_columns_pair_table(i).xml_column.default_value || '_')) then
        -- 2.1.2.5 default_value
        vt_mod_col_def_val_table.extend;
        --
        vt_mod_col_def_val_table(vt_mod_col_def_val_table.last) := t_column(vt_columns_pair_table(i).xml_column.name,
                                                                            vt_columns_pair_table(i).xml_column.type,
                                                                            vt_columns_pair_table(i).xml_column.char_length,
                                                                            vt_columns_pair_table(i).xml_column.data_precision,
                                                                            vt_columns_pair_table(i).xml_column.data_scale,
                                                                            vt_columns_pair_table(i).xml_column.char_used,
                                                                            vt_columns_pair_table(i).xml_column.nullable,
                                                                            vt_columns_pair_table(i).xml_column.default_value,
                                                                            vt_columns_pair_table(i).xml_column.virtual_expression,
                                                                            vt_columns_pair_table(i).xml_column.comments);
      -- 2.1.3. drop columns
      elsif (vt_columns_pair_table(i).xml_column.name is null) then
        vt_drop_columns_table.extend;
        vt_drop_columns_table(vt_drop_columns_table.last) := t_column(vt_columns_pair_table(i).db_column.name,
                                                                      null, null, null, null, null, null, null, null, null);
      end if;
      i := vt_columns_pair_table.next(i);
    end loop;
    --
    -- 2.1.4. add columns
    vv_stage := '2.1.4. add columns';
    i := vt_add_columns_table.first;
    while i is not null
    loop
      vc_add_columns_sql := vc_add_columns_sql || vt_add_columns_table(i).mf_get_add_column_string || ',';
      i := vt_add_columns_table.next(i);
    end loop;
    --
    vc_add_columns_sql := trim(trim(both ',' from vc_add_columns_sql));
    if (length(vc_add_columns_sql) > 0) then
      vc_sql := 'alter table ' || vv_table_owner || '.' || vv_table_name;
      vc_sql := vc_sql || CRLF || ' add (' || vc_add_columns_sql || ')';
      --
      pkg_log.sp_log_message(pv_module => cv_module_name, pv_text => vv_stage, pv_clob => vc_sql, pb_is_output => cb_is_output);
      execute immediate vc_sql;
    end if;
    -- 2.1.5. modify
    -- if record presented in VT_MODIFY_COLUMNS_TABLE collection then we 100% sure that this column should be changed
    vv_stage := '2.1.5. modify';
    i := vt_modify_columns_table.first;
    while i is not null
    loop
      vc_modify_columns_sql := vc_modify_columns_sql || vt_modify_columns_table(i).mf_get_modify_column_string || ',';
      i := vt_modify_columns_table.next(i);
    end loop;
    --
    vc_modify_columns_sql := trim(trim(both ',' from vc_modify_columns_sql));
    if (length(vc_modify_columns_sql) > 0) then
      vc_sql := 'alter table ' || vv_table_owner || '.' || vv_table_name;
      vc_sql := vc_sql || CRLF || ' modify (' || vc_modify_columns_sql || ')';
      --
      pkg_log.sp_log_message(pv_module => cv_module_name, pv_text => vv_stage, pv_clob => vc_sql, pb_is_output => cb_is_output);
      execute immediate vc_sql;
    end if;
    -- 2.1.6. modify default value
    vv_stage := '2.1.6. modify default value';
    i := vt_mod_col_def_val_table.first;
    while i is not null
    loop
      vc_mod_col_def_val_sql := vc_mod_col_def_val_sql || vt_mod_col_def_val_table(i).mf_get_mod_dev_val_string || ',';
      i := vt_mod_col_def_val_table.next(i);
    end loop;
    --
    vc_mod_col_def_val_sql := trim(trim(both ',' from vc_mod_col_def_val_sql));
    if (length(vc_mod_col_def_val_sql) > 0) then
      vc_sql := 'alter table ' || vv_table_owner || '.' || vv_table_name;
      vc_sql := vc_sql || CRLF || ' modify (' || vc_mod_col_def_val_sql || ')';
      --
      pkg_log.sp_log_message(pv_module => cv_module_name, pv_text => vv_stage, pv_clob => vc_sql, pb_is_output => cb_is_output);
      execute immediate vc_sql;
    end if;
    -- 2.1.7. drop columns
    vv_stage := '2.1.7. drop columns';
    i := vt_drop_columns_table.first;
    while i is not null
    loop
      vc_drop_columns_sql := vc_drop_columns_sql || vt_drop_columns_table(i).mf_get_drop_column_string || ',';
      i := vt_drop_columns_table.next(i);
    end loop;
    --
    vc_drop_columns_sql := trim(trim(both ',' from vc_drop_columns_sql));
    if (length(vc_drop_columns_sql) > 0) then
      vc_sql := 'alter table ' || vv_table_owner || '.' || vv_table_name;
      vc_sql := vc_sql || CRLF || ' set unused (' || vc_drop_columns_sql || ')';
      --
      pkg_log.sp_log_message(pv_module => cv_module_name, pv_text => vv_stage, pv_clob => vc_sql, pb_is_output => cb_is_output);
      execute immediate vc_sql;
    end if;
    --
    -- 2.2. comments
    -- 2.2.1. table comments
    vv_stage := '2.2.1. table comments';
    vc_sql := 'comment on table ' || vv_table_owner || '.' || vv_table_name || ' is ''' || vv_table_comments || '''';
    pkg_log.sp_log_message(pv_module => cv_module_name, pv_text => vv_stage, pv_clob => vc_sql, pb_is_output => cb_is_output);
    execute immediate vc_sql;
    -- 2.2.2. columns comments
    vv_stage := '2.2.2. columns comments';
    i := vt_columns_pair_table.first;
    while i is not null
    loop
      if (vt_columns_pair_table(i).xml_column.name is not null) then
        vc_sql := 'comment on column ' || vv_table_owner || '.' || vv_table_name || '.' || vt_columns_pair_table(i).xml_column.name || ' is ''' || vt_columns_pair_table(i).xml_column.comments || '''';
        --
        pkg_log.sp_log_message(pv_module => cv_module_name, pv_text => vv_stage, pv_clob => vc_sql, pb_is_output => cb_is_output);
        execute immediate vc_sql;
        --
        i := vt_columns_pair_table.next(i);
      end if;
    end loop;
    -- 2.3 constraints list
    select t_constraint(name, type, columns_list, foreign_owner, foreign_table, foreign_columns_list, condition, delete_rule, status, validated)
    bulk collect into vt_db_constraints_table
    from (select upper(name) as name,
                 case
                   when type = 'P' then 'PRIMARY KEY'
                   when type = 'U' then 'UNIQUE'
                   when type = 'R' then 'FOREIGN KEY'
                   when type = 'C' then 'CHECK'
                 end as type,
                 upper(columns_list) as columns_list,
                 upper(foreign_owner) as foreign_owner,
                 upper(foreign_table) as foreign_table,
                 upper(foreign_columns_list) as foreign_columns_list,
                 upper(condition) as condition,
                 upper(delete_rule) as delete_rule,
                 case
                   when status = 'ENABLED' then 'ENABLE'
                   when status = 'DISABLED' then 'DISABLE'
                   else null
                 end as status,
                 case
                   when validated = 'VALIDATED' then 'VALIDATE'
                   when validated = 'NOT VALIDATED' then 'NOVALIDATE'
                   else null
                 end as validated
          from (select con.constraint_name as name, 
                       con.constraint_type as type, 
                       listagg(c_i.column_name, ',') within group (order by c_i.position) as columns_list,
                       c_r.owner as foreign_owner,
                       c_r.table_name as foreign_table,
                       listagg(c_r.column_name, ',') within group (order by c_r.position) as foreign_columns_list,
                       pkg_utils.sf_get_search_condition(con.owner, con.constraint_name) as condition,
                       con.delete_rule,
                       con.status,
                       con.validated
                from all_constraints con,
                     all_cons_columns c_i,
                     all_cons_columns c_r
                where con.owner             = c_i.owner
                  and con.constraint_name   = c_i.constraint_name
                  and con.table_name        = c_i.table_name
                  and c_i.position          = c_r.position
                  and con.r_constraint_name = c_r.constraint_name
                  and con.r_owner           = c_r.owner
                  and con.constraint_type   = 'R'
                  and con.table_name        = vv_table_name
                  and con.owner             = vv_table_owner
                group by con.constraint_name, con.constraint_type,  c_r.owner, c_r.table_name, pkg_utils.sf_get_search_condition(con.owner, con.constraint_name), con.delete_rule, con.status, con.validated
                union all
                select con.constraint_name as name, 
                       con.constraint_type as type, 
                       listagg(c_i.column_name, ',') within group (order by c_i.position) as columns_list,
                       null as foreign_owner,
                       null as foreign_table,
                       null as foreign_columns_list,
                       pkg_utils.sf_get_search_condition(con.owner, con.constraint_name) as condition,
                       con.delete_rule,
                       con.status,
                       con.validated
                from all_constraints con,
                     all_cons_columns c_i
                where con.owner           = c_i.owner
                  and con.constraint_name = c_i.constraint_name
                  and con.table_name      = c_i.table_name
                  and con.constraint_type in ('P', 'U')
                  and con.table_name      = vv_table_name
                  and con.owner           = vv_table_owner
                group by con.constraint_name, con.constraint_type, pkg_utils.sf_get_search_condition(con.owner, con.constraint_name), con.delete_rule, con.status, con.validated
                union all
                select con.constraint_name as name, 
                       con.constraint_type as type, 
                       null columns_list,
                       null as foreign_owner,
                       null as foreign_table,
                       null as foreign_columns_list,
                       pkg_utils.sf_get_search_condition(con.owner, con.constraint_name) as condition,
                       null delete_rule,
                       con.status,
                       con.validated
                from all_constraints con
                where con.constraint_type = 'C'
                  and con.table_name = vv_table_name
                  and con.owner = vv_table_owner
                  and pkg_utils.sf_get_search_condition(pv_owner => con.owner,
                                                        pv_constraint_name => con.constraint_name) not in ( select '"' || col.column_name || '" IS NOT NULL'
                                                                                                            from all_tab_columns col
                                                                                                            where col.owner = vv_table_owner
                                                                                                              and col.table_name = vv_table_name
                                                                                                              and col.nullable = 'N')));
    --
    vt_add_constraints_table := vt_xml_constraints_table multiset except vt_db_constraints_table;
    vt_drop_constraints_table := vt_db_constraints_table multiset except vt_xml_constraints_table;
    --
    select t_constraint(xml.name, xml.type, xml.columns_list, xml.foreign_owner, xml.foreign_table, xml.foreign_columns_list, xml.condition, xml.delete_rule, xml.status, xml.validated)
    bulk collect into vt_mod_constraints_table
    from table(vt_xml_constraints_table) xml, table(vt_db_constraints_table) db
    where upper(xml.name || '_' ||
                xml.type || '_' ||
                xml.columns_list || '_' ||
                xml.foreign_owner || '_' ||
                xml.foreign_table || '_' ||
                xml.foreign_columns_list || '_' ||
                xml.condition || '_' || xml.delete_rule) = upper(db.name || '_' || db.type || '_' || db.columns_list || '_' || db.foreign_owner || '_' || db.foreign_table || '_' || db.foreign_columns_list || '_' || db.condition || '_' || db.delete_rule)
      and xml.status || '_' ||
          xml.validated != db.status || '_' || db.validated;
    -- 2.3.1. drop constraints
    vv_stage := '2.3.1. drop constraints';
    i := vt_drop_constraints_table.first;
    while i is not null
    loop
      vc_sql := 'alter table ' || vv_table_owner || '.' || vv_table_name || vt_drop_constraints_table(i).mf_get_drop_string;
      --
      pkg_log.sp_log_message(pv_module => cv_module_name, pv_text => vv_stage, pv_clob => vc_sql, pb_is_output => cb_is_output);
      execute immediate vc_sql;
      --
      i := vt_drop_constraints_table.next(i);
    end loop;    
    -- 2.3.2. create constraints
    vv_stage := '2.3.2. create constraints';
    i := vt_add_constraints_table.first;
    while i is not null
    loop
      vc_sql := 'alter table ' || vv_table_owner || '.' || vv_table_name || vt_add_constraints_table(i).mf_get_create_string;
      --
      pkg_log.sp_log_message(pv_module => cv_module_name, pv_text => vv_stage, pv_clob => vc_sql, pb_is_output => cb_is_output);
      execute immediate vc_sql;
      --
      i := vt_add_constraints_table.next(i);
    end loop;    
    -- 2.3.3. modify constraints
    vv_stage := '2.3.3. modify constraints';
    i := vt_mod_constraints_table.first;
    while i is not null
    loop
      vc_sql := 'alter table ' || vv_table_owner || '.' || vv_table_name || vt_add_constraints_table(i).mf_get_modify_string;
      --
      pkg_log.sp_log_message(pv_module => cv_module_name, pv_text => vv_stage, pv_clob => vc_sql, pb_is_output => cb_is_output);
      execute immediate vc_sql;
      --
      i := vt_mod_constraints_table.next(i);
    end loop;    
    -- 2.4 indexes list
    select t_index(name, type, uniqueness, clause, visibility)
    bulk collect into vt_db_indexes_table
    from (select c.index_name as name,
                 case
                   when i.index_type = 'BITMAP' then i.index_type
                   else null
                 end as type,
                 case
                   when i.uniqueness = 'UNIQUE' then i.uniqueness
                   else null
                 end as uniqueness,
                 listagg(c.column_name, ',') within group (order by c.column_position) as clause,
                 case
                   when i.visibility = 'INVISIBLE' then i.visibility
                   else 'VISIBLE'
                 end as visibility
          from all_indexes i, all_ind_columns c
          where i.owner = c.index_owner
            and c.index_name = i.index_name
            and c.table_name = vv_table_name
            and i.owner = vv_table_owner
            and i.index_name not in (select constraint_name from all_constraints where constraint_type in ('U', 'P') and table_name = vv_table_name and owner = vv_table_owner)
          group by c.index_name, i.index_type, i.uniqueness, i.visibility);
    --
    vt_add_indexes_table  := vt_xml_indexes_table multiset except vt_db_indexes_table;
    vt_drop_indexes_table := vt_db_indexes_table multiset except vt_xml_indexes_table;
    --
    select t_index(xml.name, xml.type, xml.uniqueness, xml.clause, xml.visibility)
    bulk collect into vt_mod_indexes_table
    from table(vt_xml_indexes_table) xml, table(vt_db_indexes_table) db
    where upper(xml.name || '_' || xml.type || '_' || xml.uniqueness || '_' || xml.clause) = upper(db.name || '_' || db.type || '_' || db.uniqueness || '_' || db.clause)
      and xml.visibility != db.visibility;
    -- 2.4.1. drop indexes
    vv_stage := '2.4.1. drop indexes';
    i := vt_drop_indexes_table.first;
    while i is not null
    loop
      vc_sql := 'drop index ' || vv_table_owner || '.' || vt_drop_indexes_table(i).name;
      --
      pkg_log.sp_log_message(pv_module => cv_module_name, pv_text => vv_stage, pv_clob => vc_sql, pb_is_output => cb_is_output);
      execute immediate vc_sql;
      --
      i := vt_drop_indexes_table.next(i);
    end loop;
    -- 2.4.2. create indexes
    vv_stage := '2.4.2. create indexes';
    i := vt_add_indexes_table.first;
    while i is not null
    loop
      vc_sql := 'create ' || vt_add_indexes_table(i).type || ' ' || vt_add_indexes_table(i).uniqueness || ' index ' || vt_add_indexes_table(i).name || ' on ' || vv_table_name || ' (' || vt_add_indexes_table(i).clause || ') ' || vt_add_indexes_table(i).visibility;
      --
      pkg_log.sp_log_message(pv_module => cv_module_name, pv_text => vv_stage, pv_clob => vc_sql, pb_is_output => cb_is_output);
      execute immediate vc_sql;
      --
      i := vt_add_indexes_table.next(i);
    end loop;
    -- 2.4.3. modify indexes
    vv_stage := '2.4.3. modify indexes';
    i := vt_mod_indexes_table.first;
    while i is not null
    loop
      vc_sql := 'alter index ' || vv_table_owner || '.' || vt_mod_indexes_table(i).name || ' ' || vt_mod_indexes_table(i).visibility;
      --
      pkg_log.sp_log_message(pv_module => cv_module_name, pv_text => vv_stage, pv_clob => vc_sql, pb_is_output => cb_is_output);
      execute immediate vc_sql;
      --
      i := vt_mod_indexes_table.next(i);
    end loop;    
  end if;
  --
  vv_stage := 'completed successfully.';
  pkg_log.sp_log_message(pv_module => cv_module_name, pv_text => vv_stage, pb_is_output => cb_is_output);
exception
  when others then
    vv_stage := 'completed with error.';
    pkg_log.sp_log_message(pv_module => cv_module_name, pv_text => vv_stage, pv_clob => dbms_utility.format_error_stack || CRLF || dbms_utility.format_error_backtrace, pv_type => 'E', pb_is_output => cb_is_output);
    raise_application_error (-20002, dbms_utility.format_error_stack || CRLF || dbms_utility.format_error_backtrace);
end sp_deploy_table_by_xml_struct;

end pkg_deploy;
/
