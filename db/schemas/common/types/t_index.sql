create or replace type t_index is object(
  name       varchar2(30),
  type       varchar2(30), -- NORMAL/BITMAP
  uniqueness varchar2(30), -- UNIQUE/NONUNIQUE
  clause     varchar2(4000),
  visibility varchar2(9), -- VISIBLE/INVISIBLE
  map member function equals return varchar2,
member function mf_log return varchar2)
/

create or replace type body t_index is

---------------------------------------
map member function equals return varchar2 as
begin
  return upper(name || '_' || type || '_' || uniqueness || '_' || clause);
end equals;

---------------------------------------
member function mf_log return varchar2 as
begin
  return upper(name || '_' || type || '_' || uniqueness || '_' || clause || '_' || visibility);
end mf_log;

end;
/
