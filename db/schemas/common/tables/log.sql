-- Create table
create table LOG
(
  dtm       TIMESTAMP(6) default SYSTIMESTAMP not null,
  batch_id  NUMBER(12) not null,
  text      VARCHAR2(4000),
  clob_text CLOB,
  type      VARCHAR2(1) default 'I' not null,
  qty       NUMBER(16)
);
-- Add comments to the table 
comment on table LOG
  is 'log';
-- Add comments to the columns 
comment on column LOG.type
  is 'I - info, W - warning, E - error';
-- Create/Recreate primary, unique and foreign key constraints 
alter table LOG
  add constraint PK_LOG primary key (DTM);
-- Create/Recreate check constraints 
alter table LOG
  add constraint CHK_LOG
  check (TYPE IN ('I', 'W', 'E'));
