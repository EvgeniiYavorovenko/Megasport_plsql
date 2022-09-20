create or replace PACKAGE body ukr_api AS

--------\ Get token from settings /--------
function get_token(
    v_jur number, 
    v_param varchar2, 
    v_dev varchar2
)return varchar2 as
    v_token varchar2(1000);
begin
    select val into v_token
    from UP_SETTINGS
    where doc = to_char(v_jur) and param = v_param and isdev=v_dev;
    
    return v_token;
end get_token;

--------\ Formation URL /--------
function get_url(
    v_type varchar2,
    v_app_name varchar2,
    v_request varchar2,
    v_dev varchar2,
    v_token varchar2 default null
)return varchar2 as
    v_url varchar2(1000);
begin
    
    if v_dev = 'Y' then
        if v_type = 'forms' then
            v_url := 'http://localhost:8440/dev/forms/';       
        else
            v_url := 'http://localhost:8440/dev/';
        end if;

    else
        if v_type = 'forms' then
            v_url := 'http://localhost:8440/prod/forms/';       
        else
            v_url := 'http://localhost:8440/prod/';
        end if;    
    end if;
    
    v_url := v_url||v_app_name||'/'||v_request;
    
    if v_token is not null then
        v_url := v_url||'?token='||v_token;
    end if;
    
    return v_url;
end get_url;

--------\ Get bank info /--------
function get_jur_info(
    v_jur number, 
    v_param varchar2
)return varchar2 as
    v_val varchar2(1000);
begin
    case v_param
        when 'NAME' then
            select name into v_val
            from JUR
            where id=v_jur;
        when 'EDRPOU' then
            select EDRPOU into v_val
            from JUR
            where id=v_jur;
        when 'INN' then
            select INN into v_val
            from JUR
            where id=v_jur;
        when 'MFO' then
            select MFO into v_val
            from JUR
            where id=v_jur;
        when 'IBAN' then
            select IBAN into v_val
            from JUR
            where id=v_jur;
    end case;
    
    return v_val;
end get_jur_info;


--------\ load regions /--------
procedure load_region(
    v_manual_clob in clob default null
) as
    v_resp clob;
    tv apex_json.t_values;
    v_token varchar2(1000);
    v_cnt number;
    v_cnt_region number;
    r_region up_region%rowtype;
    v_jur number := 481;
    v_url varchar2(1000);
begin  
    -- get access token
    v_token := ukr_api.get_token(v_jur=>v_jur, v_param=>'BEARER eCom', v_dev=>v_dev);
    
    -- ������� ����� ��� �����������
    apex_web_service.g_request_headers.DELETE();
    apex_web_service.g_request_headers(1).name := 'Accept';
    apex_web_service.g_request_headers(1).value := 'application/json';
    apex_web_service.g_request_headers(2).name := 'Authorization';
    apex_web_service.g_request_headers(2).value := 'Bearer '||v_token;
      
    if v_manual_clob is not null then
        v_resp := v_manual_clob;
    else
        -- ������ url
        v_url := ukr_api.get_url(
            v_type => 'main',
            v_app_name => 'address-classifier-ws',
            v_request => 'get_regions_by_region_ua',
            v_dev => 'N'
        );
        
        -- ������ �����
        v_resp := apex_web_service.make_rest_request(
            p_url => v_url, 
            p_http_method => 'GET'
        );
    end if;
    
    -- ������� ���������
    apex_json.parse(tv, v_resp);
    
    -- ������� ������� �������� ������
    v_cnt := apex_json.get_count(p_path => 'Entries.Entry',p_values => tv);
    
    -- ����������� ������ �����
    for i in 1..nvl(v_cnt,0) loop
        r_region.REGION_ID := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].REGION_ID', p0 => i, p_values => tv);
        r_region.REGION_UA := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].REGION_UA', p0 => i, p_values => tv);
        r_region.REGION_EN := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].REGION_EN', p0 => i, p_values => tv);
        r_region.REGION_KATOTTG := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].REGION_KATOTTG', p0 => i, p_values => tv);
        r_region.REGION_KOATUU := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].REGION_KOATUU', p0 => i, p_values => tv);
        r_region.REGION_RU := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].REGION_RU', p0 => i, p_values => tv);
        
        -- ���������� �� ��������� ���� ������ 
        select count(*) into v_cnt_region
        from UP_REGION
        where REGION_ID = r_region.REGION_ID;
        
        if nvl(v_cnt_region,0) = 0 then 
            insert into UP_REGION
            values r_region;
        else
            update UP_REGION set
                REGION_UA = r_region.REGION_UA,
                REGION_EN = r_region.REGION_EN,
                REGION_KATOTTG = r_region.REGION_KATOTTG,
                REGION_KOATUU = r_region.REGION_KOATUU,
                REGION_RU = r_region.REGION_RU
            where REGION_ID = r_region.REGION_ID;
        end if;
    end loop;
    commit;
end load_region;

--------\ ������������ �������� ������� /--------
procedure load_district_by_region(
    v_region_id in varchar2, 
    v_manual_clob in clob default null
) as
    v_resp clob;
    tv apex_json.t_values;
    v_token varchar2(1000);
    v_cnt number;
    v_cnt_district number;
    r_district up_district%rowtype;
    v_jur number := 481;
    v_url varchar2(1000);
begin  
    -- ������ ����� �������
    v_token := ukr_api.get_token(v_jur=>v_jur, v_param=>'BEARER eCom', v_dev=>v_dev);
    
    -- ������� ����� ��� �����������
    apex_web_service.g_request_headers.DELETE();
    apex_web_service.g_request_headers(1).name := 'Accept';
    apex_web_service.g_request_headers(1).value := 'application/json';
    apex_web_service.g_request_headers(2).name := 'Authorization';
    apex_web_service.g_request_headers(2).value := 'Bearer '||v_token;
    
    if v_manual_clob is not null then
        v_resp := v_manual_clob;
    else
        -- ������ url
        v_url := ukr_api.get_url(
            v_type => 'main',
            v_app_name => 'address-classifier-ws',
            v_request => 'get_districts_by_region_id_and_district_ua',
            v_dev => 'N'
        );
        
        -- ������ �����
        v_resp := apex_web_service.make_rest_request(
            p_url => v_url|| '?region_id='||v_region_id, 
            p_http_method => 'GET'
        );
    end if;
    
    -- ������� ���������
    apex_json.parse(tv, v_resp);
    
    -- ������� ������� �������� ������
    v_cnt := apex_json.get_count(p_path => 'Entries.Entry',p_values => tv);
    
    -- ����������� ������ �����
    for i in 1..nvl(v_cnt,0) loop
        r_district.DISTRICT_ID := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].DISTRICT_ID', p0 => i, p_values => tv);
        r_district.REGION_ID := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].REGION_ID', p0 => i, p_values => tv);
        r_district.DISTRICT_KOATUU := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].DISTRICT_KOATUU', p0 => i, p_values => tv);
        r_district.DISTRICT_KATOTTG := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].DISTRICT_KATOTTG', p0 => i, p_values => tv);
        r_district.DISTRICT_EN := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].DISTRICT_EN', p0 => i, p_values => tv);
        r_district.DISTRICT_UA := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].DISTRICT_UA', p0 => i, p_values => tv);
        r_district.DISTRICT_RU := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].DISTRICT_RU', p0 => i, p_values => tv);
        r_district.NEW_DISTRICT_UA := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].NEW_DISTRICT_UA', p0 => i, p_values => tv);
        
        -- ���������� �� ��������� ������ ������ 
        select count(*) into v_cnt_district
        from UP_DISTRICT
        where DISTRICT_ID = r_district.DISTRICT_ID;
        
        if nvl(v_cnt_district,0) = 0 then 
            insert into UP_DISTRICT
            values r_district;
        else
            update UP_DISTRICT set
                REGION_ID = r_district.REGION_ID,
                DISTRICT_KOATUU = r_district.DISTRICT_KOATUU,
                DISTRICT_KATOTTG = r_district.DISTRICT_KATOTTG,
                DISTRICT_EN = r_district.DISTRICT_EN,
                DISTRICT_UA = r_district.DISTRICT_UA,
                DISTRICT_RU = r_district.DISTRICT_RU,
                NEW_DISTRICT_UA = r_district.NEW_DISTRICT_UA
            where DISTRICT_ID = r_district.DISTRICT_ID;
        end if;
    end loop;
    commit;
end load_district_by_region;

--------\ ������������ �������� ��� /--------
procedure load_city_by_district(
    v_district_id in varchar2, 
    v_manual_clob in clob default null
) as
    v_resp clob;
    tv apex_json.t_values;
    v_token varchar2(1000);
    v_cnt number;
    v_cnt_city number;
    r_city up_city%rowtype;
    v_jur number := 481;
    v_url varchar2(1000);
begin  
    -- ������ ����� �������
    v_token := ukr_api.get_token(v_jur=>v_jur, v_param=>'BEARER eCom', v_dev=>v_dev);
    
    -- ������� ����� ��� �����������
    apex_web_service.g_request_headers.DELETE();
    apex_web_service.g_request_headers(1).name := 'Accept';
    apex_web_service.g_request_headers(1).value := 'application/json';
    apex_web_service.g_request_headers(2).name := 'Authorization';
    apex_web_service.g_request_headers(2).value := 'Bearer '||v_token;
    
    if v_manual_clob is not null then
        v_resp := v_manual_clob;
    else
        -- ������ url
        v_url := ukr_api.get_url(
            v_type => 'main',
            v_app_name => 'address-classifier-ws',
            v_request => 'get_city_by_region_id_and_district_id_and_city_ua',
            v_dev => 'N'
        );
        
        -- ������ �����
        v_resp := apex_web_service.make_rest_request(
            p_url => v_url || '?district_id='||v_district_id, 
            p_http_method => 'GET'
        );
    end if;
    
    -- ������� ���������
    apex_json.parse(tv, v_resp);
    
    -- ������� ������� �������� ������
    v_cnt := apex_json.get_count(p_path => 'Entries.Entry',p_values => tv);
    
    -- ����������� ������ �����
    for i in 1..nvl(v_cnt,0) loop
        r_city.CITY_ID := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].CITY_ID', p0 => i, p_values => tv);
        r_city.CITY_UA := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].CITY_UA', p0 => i, p_values => tv);
        r_city.POPULATION := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].POPULATION', p0 => i, p_values => tv);
        r_city.CITY_KATOTTG := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].CITY_KATOTTG', p0 => i, p_values => tv);
        r_city.CITY_RU := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].CITY_RU', p0 => i, p_values => tv);
        r_city.OLDCITY_RU := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].OLDCITY_RU', p0 => i, p_values => tv);
        r_city.SHORTCITYTYPE_EN := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].SHORTCITYTYPE_EN', p0 => i, p_values => tv);
        r_city.CITYTYPE_UA := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].CITYTYPE_UA', p0 => i, p_values => tv);
        r_city.OLDCITY_UA := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].OLDCITY_UA', p0 => i, p_values => tv);
        r_city.CITY_EN := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].CITY_EN', p0 => i, p_values => tv);
        r_city.CITYTYPE_RU := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].CITYTYPE_RU', p0 => i, p_values => tv);
        r_city.CITY_KOATUU := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].CITY_KOATUU', p0 => i, p_values => tv);
        r_city.NAME_UA := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].NAME_UA', p0 => i, p_values => tv);
        r_city.OLDCITY_EN := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].OLDCITY_EN', p0 => i, p_values => tv);
        r_city.SHORTCITYTYPE_RU := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].SHORTCITYTYPE_RU', p0 => i, p_values => tv);
        r_city.CITYTYPE_EN := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].CITYTYPE_EN', p0 => i, p_values => tv);
        r_city.SHORTCITYTYPE_UA := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].SHORTCITYTYPE_UA', p0 => i, p_values => tv);
        r_city.LATTITUDE := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].LATTITUDE', p0 => i, p_values => tv);
        r_city.LONGITUDE := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].LONGITUDE', p0 => i, p_values => tv);
        r_city.OWNOF := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].OWNOF', p0 => i, p_values => tv);
        r_city.DISTRICT_ID := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].DISTRICT_ID', p0 => i, p_values => tv);
        r_city.REGION_ID := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].REGION_ID', p0 => i, p_values => tv); 
        
        -- ���������� �� ��������� ������ ���� 
        select count(*) into v_cnt_city
        from UP_CITY
        where CITY_ID = r_city.CITY_ID;
        
        if nvl(v_cnt_city,0) = 0 then 
            insert into UP_CITY
            values r_city;
        else
            update UP_CITY set
                CITY_UA = r_city.CITY_UA,
                POPULATION = r_city.POPULATION,
                CITY_KATOTTG = r_city.CITY_KATOTTG,
                CITY_RU = r_city.CITY_RU,
                OLDCITY_RU = r_city.OLDCITY_RU,
                SHORTCITYTYPE_EN = r_city.SHORTCITYTYPE_EN,
                CITYTYPE_UA = r_city.CITYTYPE_UA,
                OLDCITY_UA = r_city.OLDCITY_UA,
                CITY_EN = r_city.CITY_EN,
                CITYTYPE_RU = r_city.CITYTYPE_RU,
                CITY_KOATUU = r_city.CITY_KOATUU,
                NAME_UA = r_city.NAME_UA,
                OLDCITY_EN = r_city.OLDCITY_EN,
                SHORTCITYTYPE_RU = r_city.SHORTCITYTYPE_RU,
                CITYTYPE_EN = r_city.CITYTYPE_EN,
                SHORTCITYTYPE_UA = r_city.SHORTCITYTYPE_UA,
                LATTITUDE = r_city.LATTITUDE,
                LONGITUDE = r_city.LONGITUDE,
                OWNOF = r_city.OWNOF,
                DISTRICT_ID = r_city.DISTRICT_ID,
                REGION_ID = r_city.DISTRICT_ID
            where CITY_ID = r_city.CITY_ID;
        end if;
    end loop;
    commit;
end load_city_by_district;

--------\ ������������ �������� ������ /--------
procedure load_street_by_city(
    v_city_id in varchar2, 
    v_manual_clob in clob default null
) as
    v_resp clob;
    tv apex_json.t_values;
    v_token varchar2(1000);
    v_cnt number;
    v_cnt_street number;
    r_street up_street%rowtype;
    v_jur number := 481;
    v_url varchar2(1000);
begin  
    -- ������ ����� �������
    v_token := ukr_api.get_token(v_jur=>v_jur, v_param=>'BEARER eCom', v_dev=>v_dev);
    
    -- ������� ����� ��� �����������
    apex_web_service.g_request_headers.DELETE();
    apex_web_service.g_request_headers(1).name := 'Accept';
    apex_web_service.g_request_headers(1).value := 'application/json';
    apex_web_service.g_request_headers(2).name := 'Authorization';
    apex_web_service.g_request_headers(2).value := 'Bearer '||v_token;
    
    if v_manual_clob is not null then
        v_resp := v_manual_clob;
    else
        -- ������ url
        v_url := ukr_api.get_url(
            v_type => 'main',
            v_app_name => 'address-classifier-ws',
            v_request => 'get_street_by_region_id_and_district_id_and_city_id_and_street_ua',
            v_dev => 'N'
        );
        
        -- ������ �����
        v_resp := apex_web_service.make_rest_request(
            p_url => v_url || '?city_id='||v_city_id, 
            p_http_method => 'GET'
        );
    end if;
    
    -- ������� ���������
    apex_json.parse(tv, v_resp);
    
    -- ������� ������� �������� ������
    v_cnt := apex_json.get_count(p_path => 'Entries.Entry',p_values => tv);
    
    -- ����������� ������ �����
    for i in 1..nvl(v_cnt,0) loop
        r_street.STREET_ID := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].STREET_ID', p0 => i, p_values => tv);
        r_street.REGION_ID := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].REGION_ID', p0 => i, p_values => tv);
        r_street.OLDSTREET_EN := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].OLDSTREET_EN', p0 => i, p_values => tv);
        r_street.DISTRICT_ID := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].DISTRICT_ID', p0 => i, p_values => tv);
        r_street.STREET_UA := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].STREET_UA', p0 => i, p_values => tv);
        r_street.SHORTSTREETTYPE_UA := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].SHORTSTREETTYPE_UA', p0 => i, p_values => tv);
        r_street.STREETTYPE_UA := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].STREETTYPE_UA', p0 => i, p_values => tv);
        r_street.SHORTSTREETTYPE_RU := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].SHORTSTREETTYPE_RU', p0 => i, p_values => tv);
        r_street.STREETTYPE_RU := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].STREETTYPE_RU', p0 => i, p_values => tv);
        r_street.OLDSTREET_UA := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].OLDSTREET_UA', p0 => i, p_values => tv);
        r_street.STREET_EN := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].STREET_EN', p0 => i, p_values => tv);
        r_street.OLDSTREET_RU := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].OLDSTREET_RU', p0 => i, p_values => tv);
        r_street.SHORTSTREETTYPE_EN := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].SHORTSTREETTYPE_EN', p0 => i, p_values => tv);
        r_street.STREETTYPE_EN := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].STREETTYPE_EN', p0 => i, p_values => tv);
        r_street.CITY_ID := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].CITY_ID', p0 => i, p_values => tv);
        r_street.STREET_RU := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].STREET_RU', p0 => i, p_values => tv);

        -- ���������� �� ��������� ���� ������ 
        select count(*) into v_cnt_street
        from UP_STREET
        where STREET_ID = r_street.STREET_ID;
        
        if nvl(v_cnt_street,0) = 0 then 
            insert into UP_STREET
            values r_street;
        else
            update UP_STREET set
                REGION_ID = r_street.REGION_ID,
                OLDSTREET_EN = r_street.OLDSTREET_EN,
                DISTRICT_ID = r_street.DISTRICT_ID,
                STREET_UA = r_street.STREET_UA,
                SHORTSTREETTYPE_UA = r_street.SHORTSTREETTYPE_UA,
                STREETTYPE_UA = r_street.STREETTYPE_UA,
                SHORTSTREETTYPE_RU = r_street.SHORTSTREETTYPE_RU,
                STREETTYPE_RU = r_street.STREETTYPE_RU,
                OLDSTREET_UA = r_street.OLDSTREET_UA,
                STREET_EN = r_street.STREET_EN,
                OLDSTREET_RU = r_street.OLDSTREET_RU,
                SHORTSTREETTYPE_EN = r_street.SHORTSTREETTYPE_EN,
                STREETTYPE_EN = r_street.STREETTYPE_EN,
                CITY_ID = r_street.CITY_ID,
                STREET_RU = r_street.STREET_RU
            where STREET_ID = r_street.STREET_ID;
        end if;
    end loop;
    commit;
end load_street_by_city;

--------\ ������������ �������� ������� /--------
procedure load_branch_by_city(
    v_city_id in varchar2, 
    v_manual_clob in clob default null
) as
    v_resp clob;
    tv apex_json.t_values;
    v_token varchar2(1000);
    v_cnt number;
    v_cnt_branch number;
    r_branch up_branch%rowtype;
    v_jur number := 481;
    v_url varchar2(1000);
begin  
    -- ������ ����� �������
    v_token := ukr_api.get_token(v_jur=>v_jur, v_param=>'BEARER eCom', v_dev=>v_dev);
    
    -- ������� ����� ��� �����������
    apex_web_service.g_request_headers.DELETE();
    apex_web_service.g_request_headers(1).name := 'Accept';
    apex_web_service.g_request_headers(1).value := 'application/json';
    apex_web_service.g_request_headers(2).name := 'Authorization';
    apex_web_service.g_request_headers(2).value := 'Bearer '||v_token;
    
    if v_manual_clob is not null then
        v_resp := v_manual_clob;
    else
        -- ������ url
        v_url := ukr_api.get_url(
            v_type => 'main',
            v_app_name => 'address-classifier-ws',
            v_request => 'get_postoffices_by_city_id',
            v_dev => 'N'
        );
        
        -- ������ �����
        v_resp := apex_web_service.make_rest_request(
            p_url => v_url || '?district_id='||v_city_id, 
            p_http_method => 'GET'
        );
    end if;
    
    -- ������� ���������
    apex_json.parse(tv, v_resp);
    
    -- ������� ������� �������� ������
    v_cnt := apex_json.get_count(p_path => 'Entries.Entry',p_values => tv);
    
    -- ����������� ������ �����
    for i in 1..nvl(v_cnt,0) loop
        r_branch.ID := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].ID', p0 => i, p_values => tv);
        r_branch.DISTRICT_ID := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].DISTRICT_ID', p0 => i, p_values => tv);
        r_branch.TYPE_SHORT := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].TYPE_SHORT', p0 => i, p_values => tv);
        r_branch.IS_CASH := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].IS_CASH', p0 => i, p_values => tv);
        r_branch.IS_SECURITY := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].IS_SECURITY', p0 => i, p_values => tv);
        r_branch.IS_SMARTBOX := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].IS_SMARTBOX', p0 => i, p_values => tv);
        r_branch.POSTINDEX := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].POSTINDEX', p0 => i, p_values => tv);
        r_branch.MEREZA_NUMBER := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].MEREZA_NUMBER', p0 => i, p_values => tv);
        r_branch.PO_LONG := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].PO_LONG', p0 => i, p_values => tv);
        r_branch.PO_SHORT := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].PO_SHORT', p0 => i, p_values => tv);
        r_branch.PELPEREKAZY := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].PELPEREKAZY', p0 => i, p_values => tv);
        r_branch.LOCK_RU := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].LOCK_RU', p0 => i, p_values => tv);
        r_branch.TYPE_LONG := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].TYPE_LONG', p0 => i, p_values => tv);
        r_branch.TYPE_ACRONYM := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].TYPE_ACRONYM', p0 => i, p_values => tv);
        r_branch.PARENT_ID := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].PARENT_ID', p0 => i, p_values => tv);
        r_branch.TECHINDEX := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].TECHINDEX', p0 => i, p_values => tv);
        r_branch.IS_FLAGMAN := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].IS_FLAGMAN', p0 => i, p_values => tv);
        r_branch.REGION_ID := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].REGION_ID', p0 => i, p_values => tv);
        r_branch.LOCK_EN := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].LOCK_EN', p0 => i, p_values => tv);
        r_branch.PHONE := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].PHONE', p0 => i, p_values => tv);
        r_branch.LONGITUDE := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].LONGITUDE', p0 => i, p_values => tv);
        r_branch.LATTITUDE := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].LATTITUDE', p0 => i, p_values => tv);
        r_branch.IS_AUTOMATED := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].IS_AUTOMATED', p0 => i, p_values => tv);
        r_branch.LOCK_UA := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].LOCK_UA', p0 => i, p_values => tv);
        r_branch.ISVPZ := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].ISVPZ', p0 => i, p_values => tv);
        r_branch.LOCK_CODE := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].LOCK_CODE', p0 => i, p_values => tv);
        r_branch.HOUSENUMBER := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].HOUSENUMBER', p0 => i, p_values => tv);
        r_branch.IS_DHL := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].IS_DHL', p0 => i, p_values => tv);
        r_branch.CITY_ID := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].CITY_ID', p0 => i, p_values => tv);


        -- ���������� �� ��������� ������ ��������
        select count(*) into v_cnt_branch
        from UP_BRANCH
        where ID = r_branch.ID;
        
        if nvl(v_cnt_branch,0) = 0 then 
            insert into UP_BRANCH
            values r_branch;
        else
            update UP_BRANCH set
                DISTRICT_ID = r_branch.DISTRICT_ID,
                TYPE_SHORT = r_branch.TYPE_SHORT,
                IS_CASH = r_branch.IS_CASH,
                IS_SECURITY = r_branch.IS_SECURITY,
                IS_SMARTBOX = r_branch.IS_SMARTBOX,
                POSTINDEX = r_branch.POSTINDEX,
                MEREZA_NUMBER = r_branch.MEREZA_NUMBER,
                PO_LONG = r_branch.PO_LONG,
                PO_SHORT = r_branch.PO_SHORT,
                PELPEREKAZY = r_branch.PELPEREKAZY,
                LOCK_RU = r_branch.LOCK_RU,
                TYPE_LONG = r_branch.TYPE_LONG,
                TYPE_ACRONYM = r_branch.TYPE_ACRONYM,
                PARENT_ID = r_branch.PARENT_ID,
                TECHINDEX = r_branch.TECHINDEX,
                IS_FLAGMAN = r_branch.IS_FLAGMAN,
                REGION_ID = r_branch.REGION_ID,
                LOCK_EN = r_branch.LOCK_EN,
                PHONE = r_branch.PHONE,
                LONGITUDE = r_branch.LONGITUDE,
                LATTITUDE = r_branch.LATTITUDE,
                IS_AUTOMATED = r_branch.IS_AUTOMATED,
                LOCK_UA = r_branch.LOCK_UA,
                ISVPZ = r_branch.ISVPZ,
                LOCK_CODE = r_branch.LOCK_CODE,
                HOUSENUMBER = r_branch.HOUSENUMBER,
                IS_DHL = r_branch.IS_DHL,
                CITY_ID = r_branch.CITY_ID
            where ID = r_branch.ID;
        end if;
    end loop;
    commit;
end load_branch_by_city;

--------\ ������������ ������� �������� ������� /--------
procedure load_district as
    cursor cur1 is
        select region_id from up_region;
begin
    for c1 in cur1 loop
        load_district_by_region(v_region_id=>c1.region_id);
    end loop;
end load_district;

--------\ ������������ ������� �������� ��� /--------
procedure load_city as
    cursor cur1 is
        select district_id from up_district;
begin
    for c1 in cur1 loop
        load_city_by_district(v_district_id=>c1.district_id);
    end loop;
end load_city;

--------\ ������������ ������� �������� ������ /--------
procedure load_street as
    cursor cur1 is
        select city_id from up_city;
begin
    for c1 in cur1 loop
        load_street_by_city(v_city_id=>c1.city_id);
    end loop;
end load_street;

--------\ ������������ ������� �������� ������� /--------
procedure load_branch as
    cursor cur1 is
        select city_id from up_city;
begin
    for c1 in cur1 loop
        load_branch_by_city(v_city_id=>c1.city_id);
    end loop;
end load_branch;

--------\ ��������� ������ ������� �� ������ /--------
function get_house_list(
    v_street_id in varchar2
) RETURN list_t pipelined as
    return_row list_r;
    v_resp clob;
    tv apex_json.t_values;
    v_token varchar2(1000);
    v_cnt number;
    v_jur number := 481;
    v_url varchar2(1000);
begin
    
    -- ������ ����� �������
    v_token := ukr_api.get_token(v_jur=>v_jur, v_param=>'BEARER eCom', v_dev=>v_dev);
    
    -- ������� ����� ��� �����������
    apex_web_service.g_request_headers.DELETE();
    apex_web_service.g_request_headers(1).name := 'Accept';
    apex_web_service.g_request_headers(1).value := 'application/json';
    apex_web_service.g_request_headers(2).name := 'Authorization';
    apex_web_service.g_request_headers(2).value := 'Bearer '||v_token;
    
    -- ������ url
    v_url := ukr_api.get_url(
        v_type => 'main',
        v_app_name => 'address-classifier-ws',
        v_request => 'get_addr_house_by_street_id',
        v_dev => 'N'
    );
    
    -- ������ �����
    v_resp := apex_web_service.make_rest_request(
        p_url => v_url || '?street_id='||v_street_id, 
        p_http_method => 'GET'
    );
    
    -- ������� ���������
    apex_json.parse(tv,v_resp);
    
    -- ������� ������� �������� ������
    v_cnt := apex_json.get_count(p_path => 'Entries.Entry',p_values => tv);
    
    -- ����������� ������ �����
    for i in 1..nvl(v_cnt,0) loop
        return_row.disp := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].HOUSENUMBER_UA', p0 => i, p_values => tv);
        return_row.val := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].HOUSENUMBER_UA', p0 => i, p_values => tv);
        pipe row (return_row);
    end loop;
exception when others then
    null;    
end get_house_list;

--------\ ��������� ������� �� ������ �� ������� /--------
function get_index(
    v_street_id in varchar2, 
    v_housenumber in varchar2
) RETURN varchar2 as
    return_row list_r;
    v_resp clob;
    tv apex_json.t_values;
    v_token varchar2(1000);
    v_cnt number;
    v_postcode varchar2(100);
    v_jur number := 481;
    v_url varchar2(1000);
begin
    -- ������ ����� �������
    v_token := ukr_api.get_token(v_jur=>v_jur, v_param=>'BEARER eCom', v_dev=>v_dev);
    
    -- ������� ����� ��� �����������
    apex_web_service.g_request_headers.DELETE();
    apex_web_service.g_request_headers(1).name := 'Accept';
    apex_web_service.g_request_headers(1).value := 'application/json';
    apex_web_service.g_request_headers(2).name := 'Authorization';
    apex_web_service.g_request_headers(2).value := 'Bearer '||v_token;
    
    -- ������ url
    v_url := ukr_api.get_url(
        v_type => 'main',
        v_app_name => 'address-classifier-ws',
        v_request => 'get_addr_house_by_street_id',
        v_dev => 'N'
    );
    
    -- ������ �����
    v_resp := apex_web_service.make_rest_request(
        p_url => v_url || '?street_id='||v_street_id||'&housenumber='||utl_url.escape(v_housenumber, true, 'utf-8'), 
        p_http_method => 'GET'
    );
    
    insert into up_logs(crt, url, resp, enty)
    values(sysdate, v_url || '?street_id='||v_street_id||'&housenumber='||v_housenumber,  v_resp, 'get_index');
    commit;
    
    -- ������� ���������
    apex_json.parse(tv,v_resp);
    
    -- ������� ������� �������� ������
    v_cnt := apex_json.get_count(p_path => 'Entries.Entry',p_values => tv);
    
    -- ����������� ������ �����
    for i in 1..nvl(v_cnt,0) loop
        v_postcode := apex_json.get_varchar2(p_path => 'Entries.Entry[%d].POSTCODE', p0 => i, p_values => tv);
        return v_postcode;
    end loop;
exception when others then
    null;
end get_index;

--------\ �������� ������ �������� /--------
function create_adresses (
    v_jur number,
    v_region_id varchar2, 
    v_district_id varchar2, 
    v_city_id varchar2, 
    v_street_id varchar2, 
    v_house varchar2,
    v_postcode varchar2
) return number as
    v_req clob;
    v_resp clob;
    tv apex_json.t_values;
    v_token varchar2(1000);
    v_cnt number;
    r_adresses UP_ADRESSES%rowtype;
    v_region varchar2(1000); 
    v_district varchar2(1000); 
    v_city varchar2(1000);
    v_street varchar2(1000); 
    v_url varchar2(1000);
    
    v_code varchar2(100);
    v_msg_err varchar2(1000);
begin
    -- ������ ����� �������
    v_token := ukr_api.get_token(v_jur=>v_jur, v_param=>'BEARER eCom', v_dev=>v_dev);
    
    -- ������� ����� ��� �����������
    apex_web_service.g_request_headers.DELETE();
    apex_web_service.g_request_headers(1).name := 'Content-Type';
    apex_web_service.g_request_headers(1).value := 'application/json';
    apex_web_service.g_request_headers(2).name := 'Authorization';
    apex_web_service.g_request_headers(2).value := 'Bearer '||v_token;
    
    apex_json.initialize_clob_output;
    apex_json.open_object;   
        if v_region_id is not null then
            select region_ua into v_region from up_region where region_id = v_region_id;    
            apex_json.write('region',v_region);
        end if;
        
        if v_district_id is not null then
            select district_ua into v_district from up_district where district_id = v_district_id;    
            apex_json.write('district',v_district);
        end if;
        
        if v_city_id is not null then
            select city_ua into v_city from up_city where city_id = v_city_id;     
            apex_json.write('city',v_city);
        end if;
        
        if v_street_id is not null then
            select street_ua into v_street from up_street where street_id = v_street_id;    
            apex_json.write('street',v_street);
        end if;
        
        if v_house is not null then
            apex_json.write('houseNumber',v_house);
        end if;
        
        apex_json.write('postcode',v_postcode);
    apex_json.close_object;   
    v_req := apex_json.get_clob_output;
    apex_json.free_output;
    
    -- ������ url
    v_url := ukr_api.get_url(
        v_type => 'main',
        v_app_name => 'ecom/0.0.1',
        v_request => 'addresses',
        v_dev => v_dev
    );
    
    -- ������ �����
    v_resp := apex_web_service.make_rest_request(
        p_url => v_url, 
        p_http_method => 'POST',
        p_body => v_req
    );
    insert into up_logs(crt, url, req, resp, enty)
    values(sysdate, v_url, v_req, v_resp, 'create_adresses');
    commit;
    -- ������� ���������
    apex_json.parse(tv, v_resp);
    
    v_code := apex_json.get_varchar2(p_path => 'code', p_values => tv);
    if v_code is not null then
        v_msg_err := apex_json.get_varchar2(p_path => 'message', p_values => tv);
        raise_application_error(-20001, v_msg_err);
    end if;
    
    r_adresses.ID := apex_json.get_number(p_path => 'id', p_values => tv);
    r_adresses.POSTCODE := v_postcode;
    r_adresses.REGION_ID := v_region_id;
    r_adresses.DISTRICT_ID := v_district_id;
    r_adresses.CITY_ID := v_city_id;
    r_adresses.STREET_ID := v_street_id;
    r_adresses.HOUSENUMBER := v_house;
    r_adresses.DETAILEDINFO := apex_json.get_varchar2(p_path => 'detailedInfo', p_values => tv);
    r_adresses.CREATED := apex_json.get_date(p_path => 'created', p_values => tv);
    r_adresses.LASTMODIFIED := apex_json.get_date(p_path => 'lastModified', p_values => tv);
    r_adresses.COUNTRY := apex_json.get_varchar2(p_path => 'country', p_values => tv);
    
    insert into up_adresses
    values r_adresses;
    commit;
        
    return r_adresses.ID;
end create_adresses;

--------\ �������� ���������� /--------
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
) as
    v_req clob;
    v_resp clob;
    tv apex_json.t_values;
    v_token varchar2(1000);
    r_senders UP_SENDERS%rowtype;
    v_url varchar2(1000);
    v_jur_name varchar2(100);
    v_code varchar2(100);
    v_msg_err varchar2(1000);
begin
    -- ������ ����� �������
    v_token := ukr_api.get_token(v_jur=>v_jur, v_param=>'BEARER eCom', v_dev=>v_dev);
    
    -- ������� ����� ��� �����������
    apex_web_service.g_request_headers.DELETE();
    apex_web_service.g_request_headers(1).name := 'Content-Type';
    apex_web_service.g_request_headers(1).value := 'application/json';
    apex_web_service.g_request_headers(2).name := 'Authorization';
    apex_web_service.g_request_headers(2).value := 'Bearer '||v_token;
    
    apex_json.initialize_clob_output;
    apex_json.open_object;       
        apex_json.write('contactPersonName', v_last_name||' '||v_first_name||' '||v_middle_name/*||' '||v_name*/);
        select replace(replace(upper(name),'"',''),'�','') into v_jur_name
        from jur
        where id = v_jur;
        apex_json.write('name', v_jur_name);
        apex_json.write('addressId', v_address_id);
        apex_json.write('edrpou', ukr_api.get_jur_info(v_jur=>v_jur, v_param=>'EDRPOU'));
        apex_json.write('phoneNumber', v_phone_number);
        apex_json.write('type', 'COMPANY');
        apex_json.write('bankAccount', ukr_api.get_jur_info(v_jur=>v_jur, v_param=>'IBAN'));
    apex_json.close_object;   
    v_req := apex_json.get_clob_output;
    apex_json.free_output;
    
    -- ������ url
    v_url := ukr_api.get_url(
        v_type => 'main',
        v_app_name => 'ecom/0.0.1',
        v_request => 'clients',
        v_token => ukr_api.get_token(v_jur=>v_jur, v_param=>'COUNTERPARTY TOKEN', v_dev=>v_dev),
        v_dev => v_dev
    );
    
    -- ������ �����
    v_resp := apex_web_service.make_rest_request(
        p_url => v_url, 
        p_http_method => 'POST',
        p_body => v_req
    );
    
    insert into up_logs(crt, url, req, resp, enty)
    values(sysdate, v_url, v_req, v_resp, 'create_sender');
    commit;
    -- ������� ���������
    apex_json.parse(tv, v_resp);
    
    v_code := apex_json.get_varchar2(p_path => 'code', p_values => tv);
    if v_code is not null then
        v_msg_err := apex_json.get_varchar2(p_path => 'message', p_values => tv);
        raise_application_error(-20001, v_msg_err);
    end if;
    
    r_senders.NAME := apex_json.get_varchar2(p_path => 'name', p_values => tv);
    r_senders.JUR := v_jur;
    r_senders.FIRST_NAME := v_first_name;
    r_senders.MIDDLE_NAME := v_middle_name;
    r_senders.LAST_NAME := v_last_name;
    r_senders.UUID := apex_json.get_varchar2(p_path => 'uuid', p_values => tv);
    r_senders.COUNTERPARTY_UUID := apex_json.get_varchar2(p_path => 'counterpartyUuid', p_values => tv);
    r_senders.ADDRESS_ID := apex_json.get_number(p_path => 'addressId', p_values => tv);
    r_senders.PHONE_NUMBER := apex_json.get_varchar2(p_path => 'phoneNumber', p_values => tv);
    r_senders.trpoint := v_trpoint;
    r_senders.phone_id := apex_json.get_number(p_path => 'phones[1].phoneId', p_values => tv);
    r_senders.delivery_type := v_delivery_type;
    r_senders.WAREHOUSE_ID := v_warehouse;
    insert into UP_SENDERS
    values r_senders;
    commit;
end create_sender;

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
) as
    v_req clob;
    v_resp clob;
    tv apex_json.t_values;
    v_token varchar2(1000);
    r_senders UP_SENDERS%rowtype;
    v_url varchar2(1000);
    v_jur_name varchar2(1000);
    v_code varchar2(100);
    v_msg_err varchar2(1000);
begin
    -- ������ ����� �������
    v_token := ukr_api.get_token(v_jur=>v_jur, v_param=>'BEARER eCom', v_dev=>v_dev);
    
    -- ������� ����� ��� �����������
    apex_web_service.g_request_headers.DELETE();
    apex_web_service.g_request_headers(1).name := 'Content-Type';
    apex_web_service.g_request_headers(1).value := 'application/json';
    apex_web_service.g_request_headers(2).name := 'Authorization';
    apex_web_service.g_request_headers(2).value := 'Bearer '||v_token;
    
    apex_json.initialize_clob_output;
    apex_json.open_object;       
        select replace(replace(upper(name),'"',''),'�','') into v_jur_name
        from jur
        where id = v_jur;
        apex_json.write('contactPersonName', v_last_name||' '||v_first_name||' '||v_middle_name/*||' '||v_name*/);
        apex_json.write('name', v_jur_name);
        apex_json.write('addressId', v_address_id);
        apex_json.write('edrpou', ukr_api.get_jur_info(v_jur=>v_jur, v_param=>'EDRPOU'));
        apex_json.write('phoneNumber', v_phone_number);
        apex_json.write('type', 'COMPANY');
        apex_json.write('bankAccount', ukr_api.get_jur_info(v_jur=>v_jur, v_param=>'IBAN'));
        apex_json.open_array('addresses');
            apex_json.open_object;  
                 apex_json.write('addressId', v_address_id);
            apex_json.close_object;  
        apex_json.close_array;
    apex_json.close_object;   
    v_req := apex_json.get_clob_output;
    apex_json.free_output;
    
    -- ������ url
    v_url := ukr_api.get_url(
        v_type => 'main',
        v_app_name => 'ecom/0.0.1',
        v_request => 'clients/'||v_uuid,
        v_token => ukr_api.get_token(v_jur=>v_jur, v_param=>'COUNTERPARTY TOKEN', v_dev=>v_dev),
        v_dev => v_dev
    );
    
    -- ������ �����

    v_resp := apex_web_service.make_rest_request(
        p_url => v_url, 
        p_http_method => 'PUT',
        p_body => v_req
    );
    
    insert into up_logs(crt, url, req, resp, enty)
    values(sysdate, v_url, v_req, v_resp, 'edit_sender');
    commit;
    -- ������� ���������
    apex_json.parse(tv, v_resp);
    
    v_code := apex_json.get_varchar2(p_path => 'code', p_values => tv);
    if v_code is not null then
        v_msg_err := apex_json.get_varchar2(p_path => 'message', p_values => tv);
        raise_application_error(-20001, v_msg_err);
    end if;
    
    r_senders.NAME := apex_json.get_varchar2(p_path => 'name', p_values => tv);
    r_senders.JUR := v_jur;
    r_senders.FIRST_NAME := v_first_name;
    r_senders.MIDDLE_NAME := v_middle_name;
    r_senders.LAST_NAME := v_last_name;
    r_senders.UUID := apex_json.get_varchar2(p_path => 'uuid', p_values => tv);
    r_senders.COUNTERPARTY_UUID := apex_json.get_varchar2(p_path => 'counterpartyUuid', p_values => tv);
    r_senders.ADDRESS_ID := apex_json.get_number(p_path => 'addressId', p_values => tv);
    r_senders.PHONE_NUMBER := apex_json.get_varchar2(p_path => 'phoneNumber', p_values => tv);
    r_senders.trpoint := v_trpoint;
    
    r_senders.phone_id := apex_json.get_number(p_path => 'phones[1].phoneId', p_values => tv);
    r_senders.delivery_type := v_delivery_type;
    r_senders.WAREHOUSE_ID := v_warehouse;
    
    update UP_SENDERS set 
        NAME = r_senders.NAME,
        JUR = r_senders.JUR,
        FIRST_NAME = r_senders.FIRST_NAME,
        MIDDLE_NAME = r_senders.MIDDLE_NAME,
        LAST_NAME = r_senders.LAST_NAME,
        COUNTERPARTY_UUID = r_senders.COUNTERPARTY_UUID,
        ADDRESS_ID = r_senders.ADDRESS_ID,
        PHONE_NUMBER = r_senders.PHONE_NUMBER,
        TRPOINT = r_senders.TRPOINT,
        DELIVERY_TYPE = r_senders.DELIVERY_TYPE,
        WAREHOUSE_ID = r_senders.WAREHOUSE_ID,
        PHONE_ID = r_senders.PHONE_ID
    where uuid = v_uuid;
    commit;
end edit_sender;

--------\ �������� ���������� /--------
function create_recepient (
    v_jur number,
    v_first_name varchar2,
    v_last_name varchar2,
    v_middle_name varchar2,
    v_address_id number,
    v_phone_number varchar2,
    v_email varchar2
) return varchar2 as
    v_req clob;
    v_resp clob;
    tv apex_json.t_values;
    v_token varchar2(1000);
    r_recepient UP_RECEPIENTS%rowtype;
    v_url varchar2(1000);
    
    v_code varchar2(100);
    v_msg_err varchar2(1000);
begin
    -- ������ ����� �������
    v_token := ukr_api.get_token(v_jur=>v_jur, v_param=>'BEARER eCom', v_dev=>v_dev);
    
    -- ������� ����� ��� �����������
    apex_web_service.g_request_headers.DELETE();
    apex_web_service.g_request_headers(1).name := 'Content-Type';
    apex_web_service.g_request_headers(1).value := 'application/json';
    apex_web_service.g_request_headers(2).name := 'Authorization';
    apex_web_service.g_request_headers(2).value := 'Bearer '||v_token;
    
    apex_json.initialize_clob_output;
    apex_json.open_object;       
        apex_json.write('firstName', v_first_name);
        apex_json.write('middleName', v_middle_name);
        apex_json.write('lastName', v_last_name);
        apex_json.write('addressId', v_address_id);
        apex_json.write('phoneNumber', v_phone_number);
        apex_json.write('type', 'INDIVIDUAL');
        apex_json.write('email', v_email);
    apex_json.close_object;   
    v_req := apex_json.get_clob_output;
    apex_json.free_output;
    
    -- ������ url
    v_url := ukr_api.get_url(
        v_type => 'main',
        v_app_name => 'ecom/0.0.1',
        v_request => 'clients',
        v_token => ukr_api.get_token(v_jur=>v_jur, v_param=>'COUNTERPARTY TOKEN', v_dev=>v_dev),
        v_dev => v_dev
    );
    
    -- ������ �����
    v_resp := apex_web_service.make_rest_request(
        p_url => v_url, 
        p_http_method => 'POST',
        p_body => v_req
    );
    
    insert into up_logs(crt, url, req, resp, enty)
    values(sysdate, v_url, v_req, v_resp, 'create_recepient');
    commit;
    
    -- ������� ���������
    apex_json.parse(tv, v_resp);
    
    v_code := apex_json.get_varchar2(p_path => 'code', p_values => tv);
    if v_code is not null then
        v_msg_err := apex_json.get_varchar2(p_path => 'message', p_values => tv);
        raise_application_error(-20001, v_msg_err);
    end if;
    
    r_recepient.NAME := apex_json.get_varchar2(p_path => 'name', p_values => tv);
    r_recepient.FIRST_NAME := v_first_name;
    r_recepient.MIDDLE_NAME := v_middle_name;
    r_recepient.LAST_NAME := v_last_name;
    r_recepient.UUID := apex_json.get_varchar2(p_path => 'uuid', p_values => tv);
    r_recepient.COUNTERPARTY_UUID := apex_json.get_varchar2(p_path => 'counterpartyUuid', p_values => tv);
    r_recepient.ADDRESS_ID := apex_json.get_number(p_path => 'addressId', p_values => tv);
    r_recepient.PHONE_NUMBER := apex_json.get_varchar2(p_path => 'phoneNumber', p_values => tv);
    r_recepient.EMAIL := v_email;
    
    insert into UP_RECEPIENTS
    values r_recepient;
    commit;
    
    return r_recepient.UUID;
end create_recepient;

procedure create_parcel(
    v_id in number,
    v_err out varchar2
)as
    v_req clob;
    v_resp clob;
    tv apex_json.t_values;
    v_token varchar2(1000);
    r_recepient UP_RECEPIENTS%rowtype;
    v_url varchar2(1000);
    v_jur number;
    v_code varchar2(100);
    v_msg_err varchar2(1000);
    v_paidtp number;
    v_up_deluvery_type varchar2(100);
    r_parcel up_parcels%rowtype;
    cursor cur1 is
        select
            ID,
            DOC,
            WEIGHT,
            LENGTH,
            HEIGHT,
            WIDTH,
            DECLAREDPRICE,
            QUANTITY
        from UP_PARCEL_PLACE
        where doc=v_id;
begin
    select * into r_parcel from up_parcels
    where id = v_id;
    
    select sjur into v_jur from saletp
    where ordh = r_parcel.ordh and chid = r_parcel.chid and trpoint = r_parcel.trpoint;
    -- ������ ����� �������
    v_token := ukr_api.get_token(v_jur=>v_jur, v_param=>'BEARER eCom', v_dev=>v_dev);
    
    -- ������� ����� ��� �����������
    apex_web_service.g_request_headers.DELETE();
    apex_web_service.g_request_headers(1).name := 'Content-Type';
    apex_web_service.g_request_headers(1).value := 'application/json';
    apex_web_service.g_request_headers(2).name := 'Authorization';
    apex_web_service.g_request_headers(2).value := 'Bearer '||v_token;

    apex_json.initialize_clob_output;
    apex_json.open_object;       
        apex_json.open_object('sender');
            apex_json.write('uuid', r_parcel.sender_id);
        apex_json.close_object;  
        apex_json.open_object('recipient');
            apex_json.write('uuid', r_parcel.recipient_id);
        apex_json.close_object; 
        
        if nposhta_api.GET_SERVICETP_PART(v_service_tp=>r_parcel.delivery_type ,v_part=>1) = 'Doors' then
            v_up_deluvery_type := 'D2';
        else
            v_up_deluvery_type := 'W2';
        end if;
        
        if nposhta_api.GET_SERVICETP_PART(v_service_tp=>r_parcel.delivery_type ,v_part=>2) = 'Doors' then
            v_up_deluvery_type := v_up_deluvery_type||'D';
        else
            v_up_deluvery_type := v_up_deluvery_type||'W';
        end if;
        apex_json.write('deliveryType', v_up_deluvery_type);
        apex_json.open_array('parcels');
            for c1 in cur1 loop
                apex_json.open_object;
                    apex_json.write('weight', c1.weight*1000);
                    apex_json.write('length', c1.length);
                    apex_json.write('height', c1.height);
                    apex_json.write('width', c1.width);
                    apex_json.write('declaredPrice', c1.declaredPrice);
                apex_json.close_object;
            end loop;
        apex_json.close_array;
        apex_json.write('type', 'EXPRESS');
        apex_json.write('externalId', r_parcel.ordh);
        apex_json.write('onFailReceiveType', 'RETURN');
        
        select paidtp into v_paidtp
        from ordh
        where id=r_parcel.ordh;
        if v_paidtp = 8 then
            apex_json.write('postPay', r_parcel.POST_PAY);
            apex_json.write('transferPostPayToBankAccount', true);
        end if;
        
        apex_json.write('description', r_parcel.DESCRIPTION);
        if r_parcel.payer = '1' then 
            apex_json.write('paidByRecipient', false);
            apex_json.write('postPayPaidByRecipient', false);
        else
            apex_json.write('paidByRecipient', true);
            apex_json.write('postPayPaidByRecipient', true);
        end if;
        apex_json.write('sms', true);
       
        --apex_json.write('packedBySender', true);
        apex_json.write('checkOnDelivery', true); -- �������� ��� ����� �� �������
        apex_json.write('fitting�llowed', true); -- �������� ��� ����� �� �������
    apex_json.close_object;   
    v_req := apex_json.get_clob_output;
    apex_json.free_output;
    -- ������ url
    v_url := ukr_api.get_url(
        v_type => 'main',
        v_app_name => 'ecom/0.0.1',
        v_request => 'shipments',
        v_token => ukr_api.get_token(v_jur=>v_jur, v_param=>'COUNTERPARTY TOKEN', v_dev=>v_dev),
        v_dev => v_dev
    );
    
    -- ������ �����
    v_resp := apex_web_service.make_rest_request(
        p_url => v_url, 
        p_http_method => 'POST',
        p_body => v_req
    );
    
    insert into up_logs(crt, url, req, resp, enty)
    values(sysdate, v_url, v_req, v_resp, 'create_parcel');
    commit;
    
    -- ������� ���������
    apex_json.parse(tv, v_resp);
    
    v_code := apex_json.get_varchar2(p_path => 'code', p_values => tv);
    if v_code is not null then
        v_err := apex_json.get_varchar2(p_path => 'message', p_values => tv);
    else
        r_parcel.uuid := apex_json.get_varchar2(p_path => 'uuid', p_values => tv);
        r_parcel.barcode := apex_json.get_varchar2(p_path => 'barcode', p_values => tv);
        r_parcel.delivery_Price := apex_json.get_number(p_path => 'deliveryPrice', p_values => tv);
        r_parcel.raw_Delivery_Price := apex_json.get_number(p_path => 'rawDeliveryPrice', p_values => tv);
        r_parcel.return_Delivery_Price := apex_json.get_number(p_path => 'returnDeliveryPrice', p_values => tv);
        r_parcel.post_Pay_Delivery_Price := apex_json.get_number(p_path => 'postPayDeliveryPrice', p_values => tv);
        r_parcel.STATUS := apex_json.get_varchar2(p_path => 'lifecycle.status', p_values => tv);
        r_parcel.STATUS_DATE := apex_json.get_date(p_path => 'lifecycle.statusDate', p_values => tv, p_format => 'yyyy-mm-dd"T"hh24:mi:ss');
        r_parcel.DELIVERY_DATE := apex_json.get_date(p_path => 'deliveryDate', p_values => tv, p_format => 'yyyy-mm-dd"T"hh24:mi:ss');
    
        if nvl(r_parcel.is_repeat, 'N') = 'Y' then
            update up_parcels set
                uuid = r_parcel.uuid,
                barcode = r_parcel.barcode,
                delivery_Price = r_parcel.delivery_Price,
                raw_Delivery_Price = r_parcel.raw_Delivery_Price,
                return_Delivery_Price = r_parcel.return_Delivery_Price,
                post_Pay_Delivery_Price = r_parcel.post_Pay_Delivery_Price,
                STATUS = r_parcel.STATUS,
                STATUS_DATE = r_parcel.STATUS_DATE,
                DELIVERY_DATE = r_parcel.DELIVERY_DATE,
                event_code = null,
                old_uuid = r_parcel.uuid,
                old_barcode = r_parcel.barcode
            where id = v_id;  
            commit;
        else
            update up_parcels set
                uuid = r_parcel.uuid,
                barcode = r_parcel.barcode,
                delivery_Price = r_parcel.delivery_Price,
                raw_Delivery_Price = r_parcel.raw_Delivery_Price,
                return_Delivery_Price = r_parcel.return_Delivery_Price,
                post_Pay_Delivery_Price = r_parcel.post_Pay_Delivery_Price,
                STATUS = r_parcel.STATUS,
                STATUS_DATE = r_parcel.STATUS_DATE,
                DELIVERY_DATE = r_parcel.DELIVERY_DATE
            where id = v_id;  
            commit;
        end if;
    end if;
    
exception when others then
    v_err:=sqlerrm;
end create_parcel;

procedure delete_parcel(
    v_uuid in varchar,
    v_err out varchar2
)as
    v_req clob;
    v_resp clob;
    tv apex_json.t_values;
    v_token varchar2(1000);
    r_recepient UP_RECEPIENTS%rowtype;
    v_url varchar2(1000);
    v_jur number;
    v_code varchar2(100);
    v_msg_err varchar2(1000);
    v_trpoint number;
    v_ex number;
begin
    select trpoint into v_trpoint
    from up_parcels
    where uuid=v_uuid;
    
    select jur into v_jur
    from trpoint
    where id= v_trpoint;
    
    select count(*) into v_ex
    from up_parcels
    where up_reg is not null and uuid=v_uuid;
    
    if v_ex != 0 then
        ukr_api.delete_from_group(
            v_jur => v_jur,
            v_uuid => v_uuid,
            v_err => v_err
        );
    end if;
    
    -- ������ ����� �������
    v_token := ukr_api.get_token(v_jur=>v_jur, v_param=>'BEARER eCom', v_dev=>v_dev);
    
    -- ������� ����� ��� �����������
    apex_web_service.g_request_headers.DELETE();
    apex_web_service.g_request_headers(1).name := 'Content-Type';
    apex_web_service.g_request_headers(1).value := 'application/json';
    apex_web_service.g_request_headers(2).name := 'Authorization';
    apex_web_service.g_request_headers(2).value := 'Bearer '||v_token;

    -- ������ url
    v_url := ukr_api.get_url(
        v_type => 'main',
        v_app_name => 'ecom/0.0.1',
        v_request => 'shipments/'||v_uuid,
        v_token => ukr_api.get_token(v_jur=>v_jur, v_param=>'COUNTERPARTY TOKEN', v_dev=>v_dev),
        v_dev => v_dev
    );
    
    -- ������ �����
    v_resp := apex_web_service.make_rest_request(
        p_url => v_url, 
        p_http_method => 'DELETE',
        p_body => v_req
    );
    
    insert into up_logs(crt, url, req, resp, enty)
    values(sysdate, v_url, v_req, v_resp, 'delete_parcel');
    commit;
    
    -- ������� ���������
    apex_json.parse(tv, v_resp);
    
    v_code := apex_json.get_varchar2(p_path => 'code', p_values => tv);
    if v_code is not null then
        v_err := apex_json.get_varchar2(p_path => 'message', p_values => tv);
        if v_err like '%was deleted' then
            delete from up_parcels
            where uuid=v_uuid;
            commit;
        end if;
    else
        delete from up_parcels
        where uuid=v_uuid;
        commit;
    end if;
    
exception when others then
    v_err:=sqlerrm;
    insert into up_logs(crt, url, req, resp, enty)
    values(sysdate, v_url, v_req, v_err, 'delete_parcel');
    commit;
end delete_parcel;

procedure create_group(
    v_jur in number,
    v_name in varchar2,
    v_client in varchar2,
    v_trpoint in number,
    v_err out varchar2,
    v_uuid out varchar2
)as
    v_req clob;
    v_resp clob;
    tv apex_json.t_values;
    v_token varchar2(1000);
    r_group up_reg%rowtype;
    v_code varchar2(100);
    v_url varchar2(1000);
begin
    v_token := ukr_api.get_token(v_jur=>v_jur, v_param=>'BEARER eCom', v_dev=>v_dev);
    
    -- ������� ����� ��� �����������
    apex_web_service.g_request_headers.DELETE();
    apex_web_service.g_request_headers(1).name := 'Content-Type';
    apex_web_service.g_request_headers(1).value := 'application/json';
    apex_web_service.g_request_headers(2).name := 'Authorization';
    apex_web_service.g_request_headers(2).value := 'Bearer '||v_token;
    
    apex_json.initialize_clob_output;
    apex_json.open_object;       
        apex_json.write('name', v_name);
        apex_json.write('clientUuid', v_client);
        apex_json.write('type', 'EXPRESS');
    apex_json.close_object;   
    v_req := apex_json.get_clob_output;
    apex_json.free_output;
    
    -- ������ url
    v_url := ukr_api.get_url(
        v_type => 'main',
        v_app_name => 'ecom/0.0.1',
        v_request => 'shipment-groups',
        v_token => ukr_api.get_token(v_jur=>v_jur, v_param=>'COUNTERPARTY TOKEN', v_dev=>v_dev),
        v_dev => v_dev
    );
    
    -- ������ �����
    v_resp := apex_web_service.make_rest_request(
        p_url => v_url, 
        p_http_method => 'POST',
        p_body => v_req
    );
    
    insert into up_logs(crt, url, req, resp, enty)
    values(sysdate, v_url, v_req, v_resp, 'create_group');
    commit;
    
    apex_json.parse(tv, v_resp);
    
    v_code := apex_json.get_varchar2(p_path => 'code', p_values => tv);
    if v_code is not null then
        v_err := apex_json.get_varchar2(p_path => 'message', p_values => tv);
    else
        r_group.uuid := apex_json.get_varchar2(p_path => 'uuid', p_values => tv);
        r_group.name := apex_json.get_varchar2(p_path => 'name', p_values => tv);
        r_group.type := apex_json.get_varchar2(p_path => 'type', p_values => tv);        
        r_group.crt := apex_json.get_date(p_path => 'created', p_values => tv, p_format => 'yyyy-mm-dd"T"hh24:mi:ss');
        r_group.trpoint := v_trpoint;
        r_group.app_user := v('APP_USER');
        r_group.client_uuid := v_client;
        insert into up_reg
        values r_group;
        commit;
    end if;
    v_uuid := r_group.uuid;
exception when others then
v_err :=sqlerrm;
end create_group;

procedure add_to_group(
    v_jur in number,
    v_uuid in varchar,
    v_sdat in date,
    v_edat in date,
    v_trpoint in number,
    v_err out varchar
) as
    v_req clob;
    v_resp clob;
    tv apex_json.t_values;
    v_token varchar2(1000);
    r_group up_reg%rowtype;
    v_code varchar2(100);
    v_url varchar2(1000);
    
    v_barcode varchar2(100);
    v_status number;
    v_cnt number;
    
    cursor cur1 is
        select barcode from up_parcels
        where up_reg is null 
            and trunc(sysdate) between trunc(v_sdat) and trunc(v_edat) 
            and trpoint = v_trpoint;
begin
    v_token := ukr_api.get_token(v_jur=>v_jur, v_param=>'BEARER eCom', v_dev=>v_dev);
    
    -- ������� ����� ��� �����������
    apex_web_service.g_request_headers.DELETE();
    apex_web_service.g_request_headers(1).name := 'Content-Type';
    apex_web_service.g_request_headers(1).value := 'application/json';
    apex_web_service.g_request_headers(2).name := 'Authorization';
    apex_web_service.g_request_headers(2).value := 'Bearer '||v_token;
    
    apex_json.initialize_clob_output;
    apex_json.open_array;       
        for c1 in cur1 loop
            apex_json.write(c1.barcode);
        end loop;
    apex_json.close_array;   
    v_req := apex_json.get_clob_output;
    apex_json.free_output;
       
    -- ������ url
    v_url := ukr_api.get_url(
        v_type => 'main',
        v_app_name => 'ecom/0.0.1',
        v_request => 'shipment-groups/'||v_uuid||'/shipments',
        v_token => ukr_api.get_token(v_jur=>v_jur, v_param=>'COUNTERPARTY TOKEN', v_dev=>v_dev),
        v_dev => v_dev
    );
    
    -- ������ �����
    v_resp := apex_web_service.make_rest_request(
        p_url => v_url, 
        p_http_method => 'PUT',
        p_body => v_req
    );
    
    
    insert into up_logs(crt, url, req, resp, enty)
    values(sysdate, v_url, v_req, v_resp, 'add_to_group');
    commit;
    
    apex_json.parse(tv, v_resp);
    
    v_code := apex_json.get_varchar2(p_path => 'code', p_values => tv);
    if v_code is not null then
        v_err := apex_json.get_varchar2(p_path => 'message', p_values => tv);
    else
        v_cnt := apex_json.get_count(p_path => '.', p_values => tv);
        for i in 1..v_cnt loop
            v_barcode := apex_json.get_varchar2(p_path => '[%d].targetBarcode', p0=>i, p_values => tv);
            v_status := apex_json.get_number(p_path => '[%d].httpStatus', p0=>i, p_values => tv);
            
            if v_status = 200 then
                update up_parcels set up_reg=v_uuid
                where barcode=v_barcode;
            else
                v_err := v_err||apex_json.get_varchar2(p_path => '[%d].message', p0=>i, p_values => tv)||'<br>';
            end if;
        end loop;
        commit;
    end if;
exception when others then
    v_err :=sqlerrm;
end add_to_group;

procedure delete_from_group(
    v_jur in number,
    v_uuid in varchar,
    v_err out varchar
) as
    v_req clob;
    v_resp clob;
    tv apex_json.t_values;
    v_token varchar2(1000);
    v_code varchar2(100);
    v_url varchar2(1000);
    
begin
    v_token := ukr_api.get_token(v_jur=>v_jur, v_param=>'BEARER eCom', v_dev=>v_dev);
    
    -- ������� ����� ��� �����������
    apex_web_service.g_request_headers.DELETE();
    apex_web_service.g_request_headers(1).name := 'Authorization';
    apex_web_service.g_request_headers(1).value := 'Bearer '||v_token;
       
    -- ������ url
    v_url := ukr_api.get_url(
        v_type => 'main',
        v_app_name => 'ecom/0.0.1',
        v_request => 'shipments/'||v_uuid||'/shipment-group',
        v_token => ukr_api.get_token(v_jur=>v_jur, v_param=>'COUNTERPARTY TOKEN', v_dev=>v_dev),
        v_dev => v_dev
    );
    
    -- ������ �����
    v_resp := apex_web_service.make_rest_request(
        p_url => v_url, 
        p_http_method => 'DELETE',
        p_body => null
    );
    
    
    insert into up_logs(crt, url, req, resp, enty)
    values(sysdate, v_url, v_req, v_resp, 'delete_from_group');
    commit;
    
    apex_json.parse(tv, v_resp);
    
    v_code := apex_json.get_varchar2(p_path => 'code', p_values => tv);
    if v_code is not null then
        v_err := apex_json.get_varchar2(p_path => 'message', p_values => tv);
    else
        update up_parcels set up_reg=null
        where uuid=v_uuid;
        commit;
    end if;
end delete_from_group;

function create_courier_order(
    v_jur in number,
    v_group_uuid in varchar,
    v_client_uuid in varchar,
    v_arrive_date in date,
    v_interval in varchar2,
    v_err out varchar
) return varchar2 as
    v_req clob;
    v_resp clob;
    tv apex_json.t_values;
    v_token varchar2(1000);
    v_code varchar2(100);
    v_url varchar2(1000);
    v_ADDRESS_ID number;
    v_PHONE_ID number;
    r_courier UP_COURIER%rowtype;
    v_cnt number;
    v_type varchar2(10);
    cursor cur1 is 
        select * from up_parcels
        where up_reg=v_group_uuid;
begin
    v_token := ukr_api.get_token(v_jur=>v_jur, v_param=>'BEARER eCom', v_dev=>v_dev);
    
    -- ������� ����� ��� �����������
    apex_web_service.g_request_headers.DELETE();
    apex_web_service.g_request_headers(1).name := 'Content-Type';
    apex_web_service.g_request_headers(1).value := 'application/json';
    apex_web_service.g_request_headers(2).name := 'Authorization';
    apex_web_service.g_request_headers(2).value := 'Bearer '||v_token;
    
    apex_json.initialize_clob_output;
    apex_json.open_object;       
        apex_json.write('clientUuid', v_client_uuid);
        
        select count(*) into v_cnt
        from up_parcels
        where up_reg=v_group_uuid;
        if v_cnt > 10 then
            v_type := 'MASS';
        else
            v_type := 'SINGLE';
        end if;
        apex_json.write('type', v_type);
        
        select ADDRESS_ID, PHONE_ID into v_ADDRESS_ID, v_PHONE_ID
        from UP_SENDERS where uuid = v_client_uuid;
        
        apex_json.write('addressId', v_ADDRESS_ID);
        apex_json.write('phoneId', v_PHONE_ID);
        apex_json.write('dropDate', to_char(v_arrive_date,'yyyy-mm-dd'));
        apex_json.write('interval', v_interval);
        apex_json.open_array('shipmentBarcodes');  
            for c1 in cur1 loop
                apex_json.write(c1.barcode);
            end loop;
        apex_json.close_array;
    apex_json.close_object;   
    v_req := apex_json.get_clob_output;
    apex_json.free_output;
    
    -- ������ url
    v_url := ukr_api.get_url(
        v_type => 'main',
        v_app_name => 'ecom/0.0.1',
        v_request => 'courier-service/orders',
        v_token => ukr_api.get_token(v_jur=>v_jur, v_param=>'COUNTERPARTY TOKEN', v_dev=>v_dev),
        v_dev => v_dev
    );
    
    -- ������ �����
    v_resp := apex_web_service.make_rest_request(
        p_url => v_url, 
        p_http_method => 'POST',
        p_body => v_req
    );
    
    
    insert into up_logs(crt, url, req, resp, enty)
    values(sysdate, v_url, v_req, v_resp, 'create_courier_order');
    commit;
    
    apex_json.parse(tv, v_resp);
    
    v_code := apex_json.get_varchar2(p_path => 'code', p_values => tv);
    if v_code is not null then
        v_err := apex_json.get_varchar2(p_path => 'message', p_values => tv);
    else
        r_courier.uuid := apex_json.get_varchar2(p_path => 'uuid', p_values => tv);
        r_courier.LAST_STATUS := apex_json.get_varchar2(p_path => 'lastStatus', p_values => tv);
        r_courier.LAST_STATUS_DATE := apex_json.get_date(p_path => 'lastStatusDate', p_values => tv, p_format => 'yyyy-mm-dd"T"hh24:mi:ss'); 
        r_courier.ORDER_NUMBER := apex_json.get_number(p_path => 'orderNumber', p_values => tv);
        r_courier.C_LAST_NAME := apex_json.get_varchar2(p_path => 'courierLastName', p_values => tv);
        r_courier.C_FIRST_NAME := apex_json.get_varchar2(p_path => 'courierFirstName', p_values => tv);
        r_courier.C_MIDDLE_NAME := apex_json.get_varchar2(p_path => 'courierMiddleName', p_values => tv);
        r_courier.C_PHONE := apex_json.get_varchar2(p_path => 'courierPhoneNumber', p_values => tv);
        r_courier.CAR_NUMBER := apex_json.get_varchar2(p_path => 'carNumber', p_values => tv);
        r_courier.CAR_TYPE := apex_json.get_varchar2(p_path => 'carType', p_values => tv);
        r_courier.CLIENT_UUID := v_client_uuid;
        r_courier.TYPE := v_type;
        r_courier.DROP_DATE := v_arrive_date;
        r_courier.INTERVAL := v_interval;
        r_courier.UP_REG := v_group_uuid;
        r_courier.APP_USER := v('APP_USER');
        
        insert into up_courier
        values r_courier;
        commit;
    end if;
    return r_courier.uuid;
end create_courier_order;

procedure get_parcel_status(
    v_jur in number,
    v_sdat in date,
    v_edat in date,
    v_err out varchar2
) as
    v_req clob;
    v_resp clob;
    tv apex_json.t_values;
    v_token varchar2(1000);
    v_code varchar2(100);
    v_url varchar2(1000);
    r_mov UP_PARCEL_MOV%rowtype;
    v_ex number;
    v_cnt number;
    i number:=0;
    v_barcodes clob;
    v_event varchar2(100);
    v_date date;
    v_ordh number;
    v_stat_ua varchar2(4000);
    v_ordh_stat number;
    v_paid_tp number;
    cursor cur1 is    
        select * from table(
            ukr_api.get_barcodes
            (
                v_jur =>v_jur,
                v_sdat =>v_sdat,
                v_edat =>v_edat
            )
        );
begin
    v_token := ukr_api.get_token(v_jur=>v_jur, v_param=>'BEARER StatusTracking', v_dev=>'N');
    
    -- ������� ����� ��� �����������
    apex_web_service.g_request_headers.DELETE();
    apex_web_service.g_request_headers(1).name := 'Content-Type';
    apex_web_service.g_request_headers(1).value := 'application/json';
    apex_web_service.g_request_headers(2).name := 'Authorization';
    apex_web_service.g_request_headers(2).value := 'Bearer '||v_token;
    
    -- ������ url
    v_url := ukr_api.get_url(
        v_type => 'main',
        v_app_name => 'status-tracking/0.0.1',
        v_request => 'statuses/last',
        v_token => null,
        v_dev => 'N'
    );
    
    for c1 in cur1 loop
        if v_dev = 'Y' then
            v_req:='["1111111111111"]';
        else
            v_req := c1.barcode;
        end if;
        
        -- ������ �����
        v_resp := apex_web_service.make_rest_request(
            p_url => v_url, 
            p_http_method => 'POST',
            p_body => v_req
        );
        
        insert into up_logs(crt, url, req, resp, enty)
        values(sysdate, v_url, v_req, v_resp, 'get_parcel_status');
        commit;
        
        apex_json.parse(tv, v_resp);
        
        v_err := apex_json.get_varchar2(p_path => 'message', p_values => tv);
        if v_err is not null then
            return;
        else
            v_cnt := apex_json.get_count(p_path => '.', p_values => tv);
            for i in 1..v_cnt loop
                r_mov.BARCODE := apex_json.get_varchar2(p_path => '[%d].barcode', p0=>i, p_values => tv);
                r_mov.STEP := apex_json.get_number(p_path => '[%d].step', p0=>i, p_values => tv);
                r_mov.DAT := apex_json.get_date(p_path => '[%d].date', p0=>i, p_values => tv, p_format => 'yyyy-mm-dd"T"hh24:mi:ss'); 
                r_mov.INDX := apex_json.get_varchar2(p_path => '[%d].index', p0=>i, p_values => tv);
                r_mov.NAME := apex_json.get_varchar2(p_path => '[%d].name', p0=>i, p_values => tv);
                r_mov.EVENT := apex_json.get_varchar2(p_path => '[%d].event', p0=>i, p_values => tv);
                r_mov.EVENT_NAME := apex_json.get_varchar2(p_path => '[%d].eventName', p0=>i, p_values => tv);
                r_mov.COUNTRY := apex_json.get_varchar2(p_path => '[%d].country', p0=>i, p_values => tv);
                r_mov.EVENT_REASON := apex_json.get_varchar2(p_path => '[%d].eventReason', p0=>i, p_values => tv);
                r_mov.EVENT_REASON_ID := apex_json.get_number(p_path => '[%d].eventReason_id', p0=>i, p_values => tv);
                r_mov.MAIL_TYPE := apex_json.get_number(p_path => '[%d].mailType', p0=>i, p_values => tv);
                r_mov.INDEX_ORDER := apex_json.get_number(p_path => '[%d].indexOrder', p0=>i, p_values => tv);
                
                select count(*) into v_ex
                from UP_PARCEL_MOV
                where barcode=r_mov.BARCODE and step = r_mov.STEP;
                
                if v_ex = 0 then
                    insert into UP_PARCEL_MOV
                    values r_mov;
                    commit;
                else
                    continue;
                end if;
                
                update up_parcels set 
                    event_code = r_mov.EVENT,
                    status_date = r_mov.DAT
                where barcode = r_mov.BARCODE;           
                
                select ordh into v_ordh
                from up_parcels
                where barcode = r_mov.BARCODE;
                
                select paidtp into v_paid_tp
                from ordh
                where id = v_ordh; 
                
                r_mov := null;
                commit;
            end loop;
            
        end if;
    end loop; 
    
end get_parcel_status;

function get_barcodes(
    v_jur number,
    v_sdat date,
    v_edat date
)return barcode_list_t pipelined as
    i number := 0;
    v_barcodes varchar2(4000);
    return_row barcode_list_r;
    
    cursor cur1 is    
        select p.barcode from up_parcels p
        left join trpoint t on p.trpoint=t.id
        where t.jur = v_jur and trunc(p.crt) between v_sdat and v_edat and nvl(event_code,0) not in ('41000','48000','41010','10602','10603', '31200', '312001');
begin
    for c1 in cur1 loop
        if i = 0 then
            v_barcodes := '"'||c1.barcode||'"';
            i := i + 1;
        else
            v_barcodes := v_barcodes||',"'||c1.barcode||'"';
            i := i + 1;
        end if;
        
        if i = 50 then
            return_row.barcode :='['||v_barcodes||']';
            pipe row(return_row);  
            return_row.barcode :=null;
            v_barcodes := null;
            i := 0;
        end if;
    end loop;
    
    if i != 0 then
        return_row.barcode := '[' || v_barcodes || ']';
        pipe row(return_row);  
    end if;
end get_barcodes;

end ukr_api;