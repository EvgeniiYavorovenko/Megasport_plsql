create or replace package paid_pkg as
    -- запис інформації про оплату розділеного платежу
    procedure set_paid_split(v_clob in clob, v_err out varchar2);

end paid_pkg;