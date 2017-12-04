create table LOG
(
  dtm       TIMESTAMP(6) default SYSTIMESTAMP not null,
  module    VARCHAR2(256) not null,
  text      VARCHAR2(4000),
  clob_text CLOB,
  type      VARCHAR2(1) default 'I' not null,
  sid       VARCHAR2(20) not null,
  qty       NUMBER(16)
)
/

comment on table LOG is 'log'
/

comment on column LOG.type is 'I - info, W - warning, E - error'
/

alter table LOG add constraint PK_LOG primary key (DTM)
/

alter table LOG add constraint CHK_LOG check (TYPE IN ('I', 'W', 'E'))
/
