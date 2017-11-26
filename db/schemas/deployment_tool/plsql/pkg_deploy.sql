create or replace package pkg_deploy is

  -- Author  : Mykola Solonskyi
  -- Created : 08/10/2014 12:21:04
  -- Purpose : Autopatic deploy table from XML structure

---------------------------------------
procedure sp_deploy_table_by_xml_struct(p_xml clob);

end pkg_deploy;
/

create or replace package body pkg_deploy is

CRLF constant char(2) := chr(13) || chr(10);

---------------------------------------
procedure sp_deploy_table_by_xml_struct(p_xml clob)
as
  vt_xml_columns_table      t_columns_table;
  vt_db_columns_table       t_columns_table;
  vt_add_columns_table      t_columns_table := t_columns_table();
  vt_modify_columns_table   t_columns_table := t_columns_table();
--  vt_rename_columns_table   t_columns_table := t_columns_table();
  vt_drop_columns_table     t_columns_table := t_columns_table();
  --
  vt_xml_constraints_table  t_constraints_table;
  vt_db_constraints_table   t_constraints_table;
  vt_add_constraints_table  t_constraints_table := t_constraints_table();
  vt_drop_constraints_table t_constraints_table := t_constraints_table();

  vt_xml_indexes_table      t_indexes_table;
  vt_db_indexes_table       t_indexes_table;
  vt_add_indexes_table      t_indexes_table := t_indexes_table();
  vt_drop_indexes_table     t_indexes_table := t_indexes_table();
  --
  vt_columns_pair_table     t_columns_pair_table;
  vv_table_owner            varchar2(30);
  vv_table_name             varchar2(30);
  vv_table_type             varchar2(30);
  vn_qty                    number;
  vc_sql                    clob := empty_clob();
  vc_add_columns_sql        clob := empty_clob();
  vc_modify_columns_sql     clob := empty_clob();
--  vc_rename_columns_sql     clob := empty_clob();
  vc_drop_columns_sql       clob := empty_clob();
begin
  --table name
  select upper(trim(owner)) as owner, upper(trim(name)) as name, upper(trim(type)) as type
  into vv_table_owner, vv_table_name, vv_table_type
  from xmltable('/table'
                 passing xmltype(p_xml)
                 columns
                    owner varchar2(30) path '@owner',
                    name  varchar2(30) path '@name',
                    type  varchar2(30) path '@type') xml;
  -- columns list
  select t_column(name, type, char_length, data_precision, data_scale, char_used, nullable, default_value, virtual_expression)
  bulk collect into vt_xml_columns_table
  from (select name,
               type,
               case
                 when type in ('VARCHAR2', 'VARCHAR', 'CHAR') then to_number(trim(regexp_substr(n, '^(\d+)\s*(\,?\s*(\d+))?(\w+)?\s*', 1, 1, 'i', 1)))
                 else 0
               end char_length,
               case
                 when type in ('NUMBER') then to_number(trim(regexp_substr(n, '^(\d+)\s*(\,?\s*(\d+))?(\w+)?\s*', 1, 1, 'i', 1)))
               end data_precision,
               decode(to_number(trim(regexp_substr(n, '^(\d+)\s*(\,?\s*(\d+))?(\w+)?\s*', 1, 1, 'i', 3))), 0, null) data_scale,
               pkg_utils.sf_normalize_char_used(
                 case
                   when type in ('VARCHAR2', 'VARCHAR', 'CHAR') then nvl(regexp_substr(n, '^(\d+)\s*(\,?\s*(\d+))?(\w+)?\s*', 1, 1, 'i', 4), 'B')
                   else regexp_substr(n, '^(\d+)\s*(\,?\s*(\d+))?(\w+)?\s*', 1, 1, 'i', 4)
                 end ) char_used,
               nullable,
--               nvl(default_value, 'null') default_value,
-- null -> null
-- '' -> null
               default_value,
               
               virtual_expression
        from (select upper(trim(name)) as name,
                             upper(trim(regexp_substr(type, '^\s*(\w+)\s*(\(\s*(.*)\s*\))?', 1, 1, 'i', 1))) type,
                             upper(trim(regexp_substr(type, '^\s*(\w+)\s*(\(\s*(.*)\s*\))?', 1, 1, 'i', 3))) n,
                             upper(nvl(trim(nullable), 'y')) as nullable,
                             upper(trim(default_value)) as default_value,
                             upper(trim(virtual_expression)) as virtual_expression
                      from xmltable('/table/columns/column'
                                     passing xmltype(p_xml)
                                     columns
                                        name               varchar2(30)   path '@name',
                                        type               varchar2(30)   path '@type',
                                        nullable           char(1)        path '@nullable',
                                        default_value      varchar2(30)   path '@default_value',
                                        virtual_expression varchar2(4000) path '@virtual_expression') xml));
  -- constraints list
  select t_constraint(name, type, columns_list, foreign_owner, foreign_table, foreign_columns_list, condition, delete_rule, validate_clause)
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
               upper(trim(validate_clause)) as validate_clause
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
                          validate_clause      varchar2(10)   path '@validate_clause') xml);
  -- indexes list
  select t_index(name, type, clause)
  bulk collect into vt_xml_indexes_table
  from (select upper(trim(name)) as name, replace(upper(trim(type)), 'NORMAL', '') as type, replace(upper(trim(clause)), ' ', '') as clause
        from xmltable('/table/indexes/index'
                       passing xmltype(p_xml)
                       columns
                          name   varchar2(30)   path '@name',
                          type   varchar2(30)   path '@type',
                          clause varchar2(4000) path '@clause') xml);
  -- partitions list

  select count(1)
  into vn_qty
  from all_tables
  where owner = vv_table_owner
    and table_name = vv_table_name;
  -- 1. create table
  if vn_qty = 0 then
    vc_sql := 'create table ' || vv_table_owner || '.' || vv_table_name || '(';
    -- columns
    for i in vt_xml_columns_table.first..vt_xml_columns_table.last
    loop
      vc_sql := vc_sql || vt_xml_columns_table(i).mf_get_add_column_string || ',';
    end loop;
    --
    vc_sql := trim(both ',' from vc_sql) || ')';
    dbms_output.put_line(vc_sql);
    --
    execute immediate vc_sql;
    --
    -- constraints
    for i in vt_xml_constraints_table.first..vt_xml_constraints_table.last
    loop
      vc_sql := 'alter table ' || vv_table_owner || '.' || vv_table_name || vt_xml_constraints_table(i).mf_get_create_string;
      dbms_output.put_line(vc_sql);
      --
      execute immediate vc_sql;
    end loop;
    --
    --indexes
    for i in vt_xml_indexes_table.first..vt_xml_indexes_table.last
    loop
      vc_sql := 'create ' || vt_xml_indexes_table(i).type || ' index ' || vt_xml_indexes_table(i).name || ' on ' || vv_table_name || ' (' || vt_xml_indexes_table(i).clause || ')';
      dbms_output.put_line(vc_sql);
      --
      execute immediate vc_sql;
    end loop;
  -- 2. modify structure
  else
    -- 2.1. columns
    select t_column(name, type, char_length, data_precision, data_scale, char_used, nullable, default_value, virtual_expression)
    bulk collect into vt_db_columns_table
    from (select column_name as name,
                 case
                   when virtual_column = 'YES' then null
                   else data_type
                 end as type,
                 char_length,
                 data_precision,
                 decode(data_scale, 0, null) data_scale,
                 pkg_utils.sf_normalize_char_used(char_used) char_used,
                 nullable,
                 case
                     when pkg_utils.sf_get_data_default(owner, table_name, column_name) is not null and virtual_column = 'NO' then pkg_utils.sf_get_data_default(owner, table_name, column_name)
                     else null
                 end default_value,
                 case
                     when pkg_utils.sf_get_data_default(owner, table_name, column_name) is not null and virtual_column = 'YES' then pkg_utils.sf_get_data_default(owner, table_name, column_name)
                     else null
                 end virtual_expression
          from all_tab_cols
          where owner = vv_table_owner
            and table_name = vv_table_name
            and column_id is not null);
    --
    select t_columns_pair(t_column(db.name, db.type, db.char_length, db.data_precision, db.data_scale, db.char_used, db.nullable, db.default_value, db.virtual_expression),
                          t_column(xml.name, xml.type, xml.char_length, xml.data_precision, xml.data_scale, xml.char_used, xml.nullable, xml.default_value, xml.virtual_expression))
    bulk collect into vt_columns_pair_table
    from table(vt_xml_columns_table) xml full join table(vt_db_columns_table) db on (xml.name = db.name);
    --
    for i in 1..vt_columns_pair_table.last
    loop
      -- 2.1.1. add colums
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
                                                                    vt_columns_pair_table(i).xml_column.virtual_expression);
      -- 2.1.2. modufy column types
      elsif ((vt_columns_pair_table(i).db_column.name = vt_columns_pair_table(i).xml_column.name) 
         and (vt_columns_pair_table(i).db_column != vt_columns_pair_table(i).xml_column)) then
        --
        dbms_output.put_line('XML name => ' || vt_columns_pair_table(i).xml_column.name || ', type => ' || vt_columns_pair_table(i).xml_column.type || ', char_length => ' || vt_columns_pair_table(i).xml_column.char_length || ', data_precision => ' || vt_columns_pair_table(i).xml_column.data_precision || ', data_scale => ' || vt_columns_pair_table(i).xml_column.data_scale || ', char_used => ' || vt_columns_pair_table(i).xml_column.char_used || ', nullable => ' || vt_columns_pair_table(i).xml_column.nullable || ', default_value => ' || vt_columns_pair_table(i).xml_column.default_value || ', virtual_expression => ' || vt_columns_pair_table(i).xml_column.virtual_expression);
        dbms_output.put_line('DB  name => ' || vt_columns_pair_table(i).db_column.name || ', type => ' || vt_columns_pair_table(i).db_column.type || ', char_length => ' || vt_columns_pair_table(i).db_column.char_length || ', data_precision => ' || vt_columns_pair_table(i).db_column.data_precision || ', data_scale => ' || vt_columns_pair_table(i).db_column.data_scale || ', char_used => ' || vt_columns_pair_table(i).db_column.char_used || ', nullable => ' || vt_columns_pair_table(i).db_column.nullable || ', default_value => ' || vt_columns_pair_table(i).db_column.default_value || ', virtual_expression => ' || vt_columns_pair_table(i).db_column.virtual_expression);
        --
        vt_modify_columns_table.extend;
        -- 2.1.2.1 initialization + name
        vt_modify_columns_table(vt_modify_columns_table.last) := t_column(vt_columns_pair_table(i).xml_column.name,
                                                                          null, null, null, null, null, null, null, null);
        -- 2.1.2.2 type != type
        if ((vt_columns_pair_table(i).db_column.type != vt_columns_pair_table(i).xml_column.type) and
            (vt_columns_pair_table(i).db_column.type is not null) and
            (vt_columns_pair_table(i).xml_column.type is not null)) then
          vt_modify_columns_table(vt_modify_columns_table.last).type           := vt_columns_pair_table(i).xml_column.type;
          vt_modify_columns_table(vt_modify_columns_table.last).char_length    := vt_columns_pair_table(i).xml_column.char_length;
          vt_modify_columns_table(vt_modify_columns_table.last).data_precision := vt_columns_pair_table(i).xml_column.data_precision;
          vt_modify_columns_table(vt_modify_columns_table.last).data_scale     := vt_columns_pair_table(i).xml_column.data_scale;
          vt_modify_columns_table(vt_modify_columns_table.last).char_used      := vt_columns_pair_table(i).xml_column.char_used;
          /*
          -- migrate column type
          -- 1. alter table add column_new
          vt_add_columns_table.extend;
          vt_add_columns_table(vt_add_columns_table.last) := t_columns(vt_columns_pair_table(i).xml_column.name || '_new',
                                                                       vt_columns_pair_table(i).xml_column.type,
                                                                       vt_columns_pair_table(i).xml_column.char_length,
                                                                       vt_columns_pair_table(i).xml_column.data_precision,
                                                                       vt_columns_pair_table(i).xml_column.data_scale,
                                                                       vt_columns_pair_table(i).xml_column.char_used,
                                                                       vt_columns_pair_table(i).xml_column.nullable,
                                                                       vt_columns_pair_table(i).xml_column.default_value,
                                                                       vt_columns_pair_table(i).xml_column.virtual_expression);
          -- 2. update data column_old -> column_new
          -- 3. drop column_old
          vt_drop_columns_table.extend;
          vt_drop_columns_table(vt_drop_columns_table.last) := t_columns(vt_columns_pair_table(i).db_column.name || '_new',
                                                                         null, null, null, null, null, null, null, null);
          -- 4. rename column_new -> column_old
          vt_rename_columns_table.extend;
          --???
          --???
          */
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
        -- 2.1.2.5 default_value
        if ((vt_columns_pair_table(i).db_column.default_value is null) and
            (vt_columns_pair_table(i).xml_column.default_value is null)) then
          vt_modify_columns_table(vt_modify_columns_table.last).default_value      := null;
        elsif ((vt_columns_pair_table(i).db_column.default_value is null) and
               (vt_columns_pair_table(i).xml_column.default_value is not null)) then
          vt_modify_columns_table(vt_modify_columns_table.last).default_value  := vt_columns_pair_table(i).xml_column.default_value;
        elsif ((vt_columns_pair_table(i).db_column.default_value is not null) and
               (vt_columns_pair_table(i).xml_column.default_value is null)) then
          vt_modify_columns_table(vt_modify_columns_table.last).type               := vt_columns_pair_table(i).xml_column.type;
          vt_modify_columns_table(vt_modify_columns_table.last).char_length        := vt_columns_pair_table(i).xml_column.char_length;
          vt_modify_columns_table(vt_modify_columns_table.last).data_precision     := vt_columns_pair_table(i).xml_column.data_precision;
          vt_modify_columns_table(vt_modify_columns_table.last).data_scale         := vt_columns_pair_table(i).xml_column.data_scale;
          vt_modify_columns_table(vt_modify_columns_table.last).char_used          := vt_columns_pair_table(i).xml_column.char_used;
          vt_modify_columns_table(vt_modify_columns_table.last).default_value      := null;
          vt_modify_columns_table(vt_modify_columns_table.last).virtual_expression := vt_columns_pair_table(i).xml_column.virtual_expression;
        elsif ((vt_columns_pair_table(i).db_column.default_value is not null) and
               (vt_columns_pair_table(i).xml_column.default_value is not null) and
               (vt_columns_pair_table(i).db_column.default_value != vt_columns_pair_table(i).xml_column.default_value)) then
          vt_modify_columns_table(vt_modify_columns_table.last).default_value  := vt_columns_pair_table(i).xml_column.default_value;
        else
          vt_modify_columns_table(vt_modify_columns_table.last).default_value  := vt_columns_pair_table(i).db_column.default_value;
        end if;
      -- 2.1.3. drop colums
      elsif (vt_columns_pair_table(i).xml_column.name is null) then
        vt_drop_columns_table.extend;
        vt_drop_columns_table(vt_drop_columns_table.last) := t_column(vt_columns_pair_table(i).db_column.name,
                                                                      null, null, null, null, null, null, null, null);
      end if;
    end loop;
    --
    -- add
    if (vt_add_columns_table.count > 0) then
      for i in vt_add_columns_table.first..vt_add_columns_table.last
      loop
        vc_add_columns_sql := vc_add_columns_sql || vt_add_columns_table(i).mf_get_add_column_string || ',';
      end loop;
      vc_add_columns_sql := trim(both ',' from vc_add_columns_sql);
      --
      vc_sql := 'alter table ' || vv_table_owner || '.' || vv_table_name;
      vc_sql := vc_sql || CRLF || ' add (' || vc_add_columns_sql || ')';
      --
      dbms_output.put_line(vc_sql);
      execute immediate vc_sql;
    end if;
    -- modify
    -- if record presented in VT_MODIFY_COLUMNS_TABLE collection then we 100% sure that this column should be changed
    if (vt_modify_columns_table.count > 0) then
      for i in vt_modify_columns_table.first..vt_modify_columns_table.last
      loop
        dbms_output.put_line('modify: name => ' || vt_modify_columns_table(i).name || ', type => ' || vt_modify_columns_table(i).type || ', char_length => ' || vt_modify_columns_table(i).char_length || ', data_precision => ' || vt_modify_columns_table(i).data_precision || ', data_scale => ' || vt_modify_columns_table(i).data_scale || ', char_used => ' || vt_modify_columns_table(i).char_used || ', nullable => ' || vt_modify_columns_table(i).nullable || ', default_value => ' || vt_modify_columns_table(i).default_value || ', virtual_expression => ' || vt_modify_columns_table(i).virtual_expression);
        vc_modify_columns_sql := vc_modify_columns_sql || vt_modify_columns_table(i).mf_get_modify_column_string || ',';
      end loop;
      vc_modify_columns_sql := trim(both ',' from vc_modify_columns_sql);
      --
      vc_sql := 'alter table ' || vv_table_owner || '.' || vv_table_name;
      vc_sql := vc_sql || CRLF || ' modify (' || vc_modify_columns_sql || ')';
      --
      dbms_output.put_line(vc_sql);
      execute immediate vc_sql;
    end if;
    --rename
    /*
    if (vt_rename_columns_table.count > 0) then
      for i in vt_rename_columns_table.first..vt_rename_columns_table.last
      loop
        vc_rename_columns_sql := vc_rename_columns_sql || CRLF || '  ' || vt_modify_columns_table(i).name || ',';
      end loop;
      vc_rename_columns_sql := trim(both ',' from vc_rename_columns_sql);
      --
      vc_sql := vc_rename_columns_sql;
      --
      --execute immediate vc_sql;
      dbms_output.put_line(vc_sql);
    end if;
    */
    -- drop
    if (vt_drop_columns_table.count > 0) then
      for i in vt_drop_columns_table.first..vt_drop_columns_table.last
      loop
        vc_drop_columns_sql := vc_drop_columns_sql || vt_drop_columns_table(i).mf_get_drop_column_string || ',';
      end loop;
      vc_drop_columns_sql := trim(both ',' from vc_drop_columns_sql);
      --
      vc_sql := 'alter table ' || vv_table_owner || '.' || vv_table_name;
      vc_sql := vc_sql || CRLF || ' set unused (' || vc_drop_columns_sql || ')';
      --
      dbms_output.put_line(vc_sql);
      execute immediate vc_sql;
    end if;
    -- 2.2 constraints list
    select t_constraint(name, type, columns_list, foreign_owner, foreign_table, foreign_columns_list, condition, delete_rule, validate_clause)
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
                 null as validate_clause
          from (select con.constraint_name as name, 
                       con.constraint_type as type, 
                       listagg(c_i.column_name, ',') within group (order by c_i.position) as columns_list,
                       c_r.owner as foreign_owner,
                       c_r.table_name as foreign_table,
                       listagg(c_r.column_name, ',') within group (order by c_r.position) as foreign_columns_list,
                       pkg_utils.sf_get_search_condition(con.owner, con.constraint_name) as condition,
                       con.delete_rule
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
                group by con.constraint_name, con.constraint_type,  c_r.owner, c_r.table_name, pkg_utils.sf_get_search_condition(con.owner, con.constraint_name), con.delete_rule
                union all
                select con.constraint_name as name, 
                       con.constraint_type as type, 
                       listagg(c_i.column_name, ',') within group (order by c_i.position) as columns_list,
                       null as foreign_owner,
                       null as foreign_table,
                       null as foreign_columns_list,
                       pkg_utils.sf_get_search_condition(con.owner, con.constraint_name) as condition,
                       con.delete_rule
                from all_constraints con,
                     all_cons_columns c_i
                where con.owner           = c_i.owner
                  and con.constraint_name = c_i.constraint_name
                  and con.table_name      = c_i.table_name
                  and con.constraint_type in ('P', 'U')
                  and con.table_name      = vv_table_name
                  and con.owner           = vv_table_owner
                group by con.constraint_name, con.constraint_type, pkg_utils.sf_get_search_condition(con.owner, con.constraint_name), con.delete_rule
                union all
                select con.constraint_name as name, 
                       con.constraint_type as type, 
                       null columns_list,
                       null as foreign_owner,
                       null as foreign_table,
                       null as foreign_columns_list,
                       pkg_utils.sf_get_search_condition(con.owner, con.constraint_name) as condition,
                       null delete_rule
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
    /*
    for i in vt_xml_constraints_table.first..vt_xml_constraints_table.last
    loop
      dbms_output.put_line(vt_xml_constraints_table(i).mf_log);
    end loop;
    --
    for i in vt_db_constraints_table.first..vt_db_constraints_table.last
    loop
      dbms_output.put_line(vt_db_constraints_table(i).mf_log);
    end loop;
    */
    vt_add_constraints_table := vt_xml_constraints_table multiset except vt_db_constraints_table;
    vt_drop_constraints_table := vt_db_constraints_table multiset except vt_xml_constraints_table;
    -- 2.2.1 drop constraints
    if vt_drop_constraints_table.count > 0 then
      for i in vt_drop_constraints_table.first..vt_drop_constraints_table.last
      loop
        vc_sql := 'alter table ' || vv_table_owner || '.' || vv_table_name || vt_drop_constraints_table(i).mf_get_drop_string;
        dbms_output.put_line(vc_sql);
        execute immediate vc_sql;
      end loop;
    end if;
    -- 2.2.2. create constraints
    if vt_add_constraints_table.count > 0 then
      for i in vt_add_constraints_table.first..vt_add_constraints_table.last
      loop
        vc_sql := 'alter table ' || vv_table_owner || '.' || vv_table_name || vt_add_constraints_table(i).mf_get_create_string;
        dbms_output.put_line(vc_sql);
        execute immediate vc_sql;
      end loop;
    end if;
    -- 2.3 indexes list
    select t_index(name, type, clause)
    bulk collect into vt_db_indexes_table
    from (select c.index_name as name,
                 case 
                   when i.uniqueness = 'NONUNIQUE' then ''
                   else i.uniqueness
                 end as type,
                 listagg(c.column_name, ',') within group (order by c.column_position) as clause
          from all_indexes i, all_ind_columns c
          where i.owner = c.index_owner
            and c.index_name = i.index_name
            and c.table_name = vv_table_name
            and i.owner = vv_table_owner
            and i.index_name not in (select constraint_name from all_constraints where constraint_type in ('U', 'P') and table_name = vv_table_name and owner = vv_table_owner)
          group by c.index_name, i.uniqueness);
    --
    vt_add_indexes_table  := vt_xml_indexes_table multiset except vt_db_indexes_table;
    vt_drop_indexes_table := vt_db_indexes_table multiset except vt_xml_indexes_table;
    -- 2.2.1 drop indexes
    if vt_drop_indexes_table.count > 0 then
      for i in vt_drop_indexes_table.first..vt_drop_indexes_table.last
      loop
        vc_sql := 'drop index ' || vv_table_owner || '.' || vv_table_name;
        dbms_output.put_line(vc_sql);
--        execute immediate vc_sql;
      end loop;
    end if;
    -- 2.2.2. create indexes
    if vt_add_indexes_table.count > 0 then
      for i in vt_add_indexes_table.first..vt_add_indexes_table.last
      loop
        vc_sql := 'create ' || vt_add_indexes_table(i).type || ' index ' || vt_add_indexes_table(i).name || ' on ' || vv_table_name || ' (' || vt_add_indexes_table(i).clause || ')';
        dbms_output.put_line(vc_sql);
        --
--        execute immediate vc_sql;
      end loop;
    end if;
  end if;
exception
  when others then
    dbms_output.put_line(vc_sql);
    raise_application_error (-20002, dbms_utility.format_error_stack || ' ' || dbms_utility.format_error_backtrace);
end sp_deploy_table_by_xml_struct;

end pkg_deploy;
/
