create or replace type t_constraint is object(
  name                 varchar2(30),
  type                 varchar2(30),
  columns_list         varchar2(4000),
  foreign_owner        varchar2(30),
  foreign_table        varchar2(30),
  foreign_columns_list varchar2(4000),
  condition            varchar2(4000),
  delete_rule          varchar2(9),
  status               varchar2(8), -- ENABLE/DISABLE
  validated            varchar2(13),-- VALIDATE/NOVALIDATE
map member function equals return varchar2,
member function mf_status_validated_to_string return varchar2,
member function mf_get_create_string return varchar2,
member function mf_get_modify_string return varchar2,
member function mf_get_drop_string return varchar2,
member function mf_log return varchar2);
/

create or replace type body t_constraint is

---------------------------------------
map member function equals return varchar2 as
begin
  return upper(self.name || '_' || self.type || '_' || self.columns_list || '_' || self.foreign_owner || '_' || self.foreign_table || '_' || self.foreign_columns_list || '_' || self.condition || '_' || self.delete_rule);
end equals;

---------------------------------------
member function mf_get_create_string return varchar2 as
  vv_value varchar2(4000) := '';
begin
  case
    when self.type in ('PRIMARY KEY', 'UNIQUE') then
      vv_value := ' add constraint ' || self.name || ' ' || self.type || ' (' || self.columns_list || ') ' || self.status || ' ' || self.validated;
    when self.type = 'FOREIGN KEY' then
      vv_value := ' add constraint ' || self.name || ' ' || self.type || ' (' || self.columns_list || ') references ' || self.foreign_table || ' ' || ' (' || self.foreign_columns_list || ') ' || self.status || ' ' || self.validated;
    when self.type = 'CHECK' then
      vv_value := ' add constraint ' || self.name || ' ' || self.type || ' (' || self.condition || ') ' || self.status || ' ' || self.validated;
    else
      vv_value := null;
  end case;
  return vv_value;
end mf_get_create_string;

---------------------------------------
member function mf_get_modify_string return varchar2 as
  vv_value varchar2(4000);
begin
  vv_value := ' ' || self.status || ' ' || self.validated || ' constraint ' || self.name;
  return vv_value;
end mf_get_modify_string;

---------------------------------------
member function mf_get_drop_string return varchar2 as
  vv_value varchar2(4000);
begin
  vv_value := ' drop constraint ' || self.name;
  return vv_value;
end mf_get_drop_string;

---------------------------------------
member function mf_status_validated_to_string return varchar2 as
  vv_value varchar2(4000);
begin
  vv_value := self.status || '_' || self.validated;
  return vv_value;
end mf_status_validated_to_string;

---------------------------------------
member function mf_log return varchar2 as
  vv_value varchar2(4000);
begin
  return name || '_' || self.type || '_' || self.columns_list || '_' || self.foreign_owner || '_' || self.foreign_table || '_' || self.foreign_columns_list || '_' || self.condition || '_' || self.delete_rule || '_' || self.status || '_' || self.validated;
  return vv_value;
end mf_log;

end;
/

