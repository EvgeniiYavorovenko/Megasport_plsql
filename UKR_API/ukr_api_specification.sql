create or replace PACKAGE ukr_api AS

v_dev varchar2(100) := 'N';

TYPE list_r IS RECORD(
    disp varchar2(100),
    val varchar2(100)
  );
  
TYPE list_t IS
TABLE OF list_r;

TYPE barcode_list_r IS RECORD(
    barcode varchar2(4000)
  );
  
TYPE barcode_list_t IS
TABLE OF barcode_list_r;

-- отримання токена --
function get_token(
    v_jur number, 
    v_param varchar2, 
    v_dev varchar2
)return varchar2;

-- отримання налаштувань --
function get_jur_info(
    v_jur number, 
    v_param varchar2
)return varchar2;

-- формування URL --
function get_url(
    v_type varchar2,
    v_app_name varchar2,
    v_request varchar2,
    v_dev varchar2,
    v_token varchar2 default null
)return varchar2;

-- завантаження довідника областей
procedure load_region(
    v_manual_clob in clob default null
);

-- завантаження довідника регіонів
procedure load_district_by_region(
    v_region_id in varchar2, 
    v_manual_clob in clob default null
);

-- завантаження довідника міст
procedure load_city_by_district(
    v_district_id in varchar2, 
    v_manual_clob in clob default null
);

-- завантаження довідника вулиць
procedure load_street_by_city(
    v_city_id in varchar2, 
    v_manual_clob in clob default null
);

-- завантаження довідника відділень
procedure load_branch_by_city(
    v_city_id in varchar2, 
    v_manual_clob in clob default null
);

-- завантаження повного довідника регіонів
procedure load_district;

-- завантаження повного довідника міст
procedure load_city;

-- завантаження повного довідника вулиць
procedure load_street;

-- завантаження повного довідника відділень
procedure load_branch;

-- отриматти список будівль вулиці
function get_house_list(
    v_street_id in varchar2
) RETURN list_t pipelined;

-- отримати список індексів, вулиці
function get_index(
    v_street_id in varchar2, 
    v_housenumber in varchar2
) RETURN varchar2;

-- створити адресу доставки --
function create_adresses (
    v_jur number,
    v_region_id varchar2, 
    v_district_id varchar2, 
    v_city_id varchar2, 
    v_street_id varchar2, 
    v_house varchar2,
    v_postcode varchar2
) return number;

-- створити відправника --
procedure create_sender (
    v_jur number,
    v_name varchar2,
    v_first_name varchar2,
    v_last_name varchar2,
    v_middle_name varchar2,
    v_address_id number,
    v_phone_number varchar2,
    v_trpoint number,
    v_delivery_type varchar2,
    v_warehouse number
);

procedure edit_sender (
    v_jur number,
    v_name varchar2,
    v_first_name varchar2,
    v_last_name varchar2,
    v_middle_name varchar2,
    v_address_id number,
    v_phone_number varchar2,
    v_trpoint number,
    v_delivery_type varchar2,
    v_warehouse number,
    v_uuid varchar2
);

-- створити отримувача --
function create_recepient (
    v_jur number,
    v_first_name varchar2,
    v_last_name varchar2,
    v_middle_name varchar2,
    v_address_id number,
    v_phone_number varchar2,
    v_email varchar2
) return varchar2;

procedure create_parcel(
    v_id in number,
    v_err out varchar2
);

procedure delete_parcel(
    v_uuid in varchar,
    v_err out varchar2
);

procedure create_group(
    v_jur number,
    v_name varchar2,
    v_client varchar2,
    v_trpoint number,
    v_err out varchar2,
    v_uuid out varchar2   
);

procedure add_to_group(
    v_jur in number,
    v_uuid in varchar,
    v_sdat in date,
    v_edat in date,
    v_trpoint in number,
    v_err out varchar
);

procedure delete_from_group(
    v_jur in number,
    v_uuid in varchar,
    v_err out varchar
);

function create_courier_order(
    v_jur in number,
    v_group_uuid in varchar,
    v_client_uuid in varchar,
    v_arrive_date in date,
    v_interval in varchar2,
    v_err out varchar
)return varchar2;

procedure get_parcel_status(
    v_jur in number,
    v_sdat in date,
    v_edat in date,
    v_err out varchar2
);

function get_barcodes(
    v_jur number,
    v_sdat date,
    v_edat date
)return barcode_list_t pipelined;

end ukr_api;