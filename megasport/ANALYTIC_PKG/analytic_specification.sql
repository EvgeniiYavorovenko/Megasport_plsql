create or replace package analytic_pkg as

-- продажі -> по магазинам
TYPE sales_by_shops_r_char IS RECORD(
    NAME varchar2(4000),
    PRICE2_SALE_SUM varchar2(100),
    PRICE2_BACK_SUM varchar2(100),
    PROC_BACK_SUM varchar2(100),
    PRICE2_SUM varchar2(100),
    DISC_SUM varchar2(100),
    COST_SUM varchar2(100),
    PR_DISC varchar2(100),
    PR varchar2(100),
    PR_cnt varchar2(100),
    CNT varchar2(100),
    sale_cnt varchar2(100),
    back_cnt varchar2(100)
  );

TYPE sales_by_shops_t_char IS
TABLE OF sales_by_shops_r_char;

function sales_by_shops(
    d1_1 date,
    d2_1 date,
    d1_2 date,
    d2_2 date,
    v_without_pkg number,
    v_add_period number
) RETURN sales_by_shops_t_char pipelined;
------------------------

-- продажі -> по брендам
function sales_by_brand(
    d1_1 date,
    d2_1 date,
    d1_2 date,
    d2_2 date,
    v_without_pkg number,
    v_add_period number
) RETURN sales_by_shops_t_char pipelined;
-------------------------

-- продажі -> по групам
function sales_by_group(
    d1_1 date,
    d2_1 date,
    d1_2 date,
    d2_2 date,
    v_without_pkg number,
    v_add_period number
) RETURN sales_by_shops_t_char pipelined;
-------------------------

-- продажі -> по сезонам
function sales_by_szon(
    d1_1 date,
    d2_1 date,
    d1_2 date,
    d2_2 date,
    v_without_pkg number,
    v_add_period number
) RETURN sales_by_shops_t_char pipelined;
-------------------------

-- отримуєм розкладені періоди по дням
TYPE date_r IS RECORD(
    p1 date,
    p2 date
  );

TYPE date_t IS
TABLE OF date_r;

function get_date_table(
    d1_1 date,
    d2_1 date,
    d1_2 date,
    d2_2 date
) RETURN date_t pipelined;
-------------------------

-- продажі -> по дням з урахуванням повернень
TYPE sales_by_day_with_back_r IS RECORD(
    DAT varchar2(1000),
    SALE_SUM varchar2(100),
    BACK_SUM varchar2(100),
    PRICE2_SUM varchar2(100),
    PR_DISC varchar2(100),
    PR varchar2(100),
    PR_ALL varchar2(100),
    PROC_BACK_SUM varchar2(100),
    CNT varchar2(100),
    sale_cnt varchar2(100),
    back_cnt varchar2(100)
  );

TYPE sales_by_day_with_back_t IS
TABLE OF sales_by_day_with_back_r;

function sales_by_day_with_back(
    d1_1 date,
    d2_1 date,
    d1_2 date,
    d2_2 date,
    v_without_pkg number,
    v_add_period number
) RETURN sales_by_day_with_back_t pipelined;
-------------------------

-- продажі -> по даті продажі
TYPE sales_by_day_r IS RECORD(
    DAT varchar2(1000),
    SALE_SUM varchar2(100),
    BACK_SUM varchar2(100),
    PRICE2_SUM varchar2(100),
    PROC_BACK_SUM varchar2(100),
    CNT varchar2(100),
    sale_cnt varchar2(100),
    back_cnt varchar2(100)
  );

TYPE sales_by_day_t IS
TABLE OF sales_by_day_r;

function sales_by_day(
    d1_1 date,
    d2_1 date,
    d1_2 date,
    d2_2 date,
    v_without_pkg number,
    v_add_period number
) RETURN sales_by_day_t pipelined;
-------------------------

function sales_by_shops_day(
    d1_1 date,
    d2_1 date,
    d1_2 date,
    d2_2 date,
    v_without_pkg number,
    v_add_period number
) RETURN sales_by_shops_t_char pipelined;
------------------------

-- продажі -> графік відправок
TYPE sales_send_char_r IS RECORD(
    CITY varchar2(4000),
    PR number
  );

TYPE sales_send_char_t IS
TABLE OF sales_send_char_r;

function sales_send_char(
    d1_1 date,
    d2_1 date,
    d1_2 date,
    d2_2 date,
    v_without_pkg number,
    v_add_period number
) RETURN sales_send_char_t pipelined;

end analytic_pkg;