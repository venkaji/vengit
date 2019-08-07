create or replace PACKAGE BODY      MROI_HR_LOCATION_CONV_PKG AS
 /* ***************************************************************************
 /*  Name:          MROI_HR_LOCATION_CONV_PKG 
 /*  Object Type:   Package BODY
 /*  Description:   Package called by HR Location Conversion
 /*
 /*  RICE Type:     Conversion
 /*  RICE ID:    	0029
 /*
 /*  Change History:
 /*  Date         Name                              Ver     Modification
 /* -----------  ----------------------             ----     -------------
 /* 05/01/2019    Vinh Giang                        1.0     Initial Creation
 /******************************************************************************/
gv_pkg_name VARCHAR2(4000) := 'MROI_HR_LOCATION_CONV_PKG';
gv_proc_name VARCHAR2(4000);


TYPE loc_tab_row IS RECORD
(
     RECORD_HEADER_ID       MROI_HR_LOCATION_HDR_STG.RECORD_HEADER_ID%TYPE
    ,INSTANCE_ID            MROI_HR_LOCATION_HDR_STG.INSTANCE_ID%TYPE
    ,TRANSACTION_ID         MROI_HR_LOCATION_HDR_STG.TRANSACTION_ID%TYPE
    ,GLOBAL_FLAG            MROI_HR_LOCATION_HDR_STG.GLOBAL_FLAG%TYPE
    ,LOCATION_NAME          MROI_HR_LOCATION_HDR_STG.LOCATION_NAME%TYPE
    ,LOCATION_DESCRIPTION   MROI_HR_LOCATION_HDR_STG.LOCATION_DESCRIPTION%TYPE
    ,ADDRESS_STYLE          MROI_HR_LOCATION_HDR_STG.ADDRESS_STYLE%TYPE
    ,ADDRESS_LINE1          MROI_HR_LOCATION_HDR_STG.ADDRESS_LINE1%TYPE
    ,ADDRESS_LINE2          MROI_HR_LOCATION_HDR_STG.ADDRESS_LINE2%TYPE
    ,ADDRESS_LINE3          MROI_HR_LOCATION_HDR_STG.ADDRESS_LINE3%TYPE
    ,CITY                   MROI_HR_LOCATION_HDR_STG.CITY%TYPE
    ,STATE                  MROI_HR_LOCATION_HDR_STG.STATE%TYPE
    ,ZIPCODE                MROI_HR_LOCATION_HDR_STG.ZIPCODE%TYPE
    ,COUNTRY                MROI_HR_LOCATION_HDR_STG.COUNTRY%TYPE
    ,TIMEZONE               MROI_HR_LOCATION_HDR_STG.TIMEZONE%TYPE
    ,SHIP_TO_SITE           MROI_HR_LOCATION_HDR_STG.SHIP_TO_SITE%TYPE
    ,RECEIVING_SITE         MROI_HR_LOCATION_HDR_STG.RECEIVING_SITE%TYPE
    ,OFFICE_SITE            MROI_HR_LOCATION_HDR_STG.OFFICE_SITE%TYPE
    ,BILL_TO_SITE           MROI_HR_LOCATION_HDR_STG.BILL_TO_SITE%TYPE
    ,INTERNAL_SITE          MROI_HR_LOCATION_HDR_STG.INTERNAL_SITE%TYPE
    ,INVENTORY_ORGANIZATION MROI_HR_LOCATION_HDR_STG.INVENTORY_ORGANIZATION%TYPE
    ,CREATION_DATE          MROI_HR_LOCATION_HDR_STG.CREATION_DATE%TYPE
    ,LAST_UPDATE_DATE       MROI_HR_LOCATION_HDR_STG.LAST_UPDATE_DATE%TYPE
    ,CREATED_BY             MROI_HR_LOCATION_HDR_STG.CREATED_BY%TYPE
    ,LAST_UPDATED_BY        MROI_HR_LOCATION_HDR_STG.LAST_UPDATED_BY%TYPE
);


PROCEDURE write_log(P_STRING VARCHAR2)
IS 
BEGIN
    FND_FILE.PUT_LINE(FND_FILE.log, P_STRING);
    DBMS_OUTPUT.PUT_LINE(P_STRING);
END write_log;

-- Procedure to write to the concurrent request output
PROCEDURE write_output(p_string VARCHAR2)
IS 
BEGIN
    fnd_file.put_line(FND_FILE.output, p_string);
    dbms_output.put_line('Output: '||p_string);
END write_output;

PROCEDURE  generate_report (p_instance_id IN NUMBER)
IS

l_total_count NUMBER;
l_error_count NUMBER;

BEGIN

    select count(*) INTO l_total_count  from mroi_tt_trx_status  where instance_id = p_instance_id;
    select count(*) INTO l_error_count  from mroi_tt_trx_status  where instance_id = p_instance_id and status_code IN (MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_ERRED_STG, MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_ERRED_OIT_API) ;

    write_output ('MROi HR Location Conversion');
    write_output ('--------------------------------------------');
    write_output ('  Total Count: ' || TO_CHAR(l_total_count));
    write_output ('  Error Count: ' || TO_CHAR(l_error_count));
    write_output ('Success Count: ' || TO_CHAR(l_total_count - l_error_count));
    write_output ('--------------------------------------------');


END generate_report;

FUNCTION parse_string (p_s VARCHAR2) RETURN VARCHAR2

IS

BEGIN

    IF  INSTR(p_s,',') = 0 OR p_s IS NULL THEN
        RETURN p_s;        
    ELSE
        RETURN ('"' || p_s || '"');
    END IF;

END parse_string;

PROCEDURE  write_outfile (p_instance_id IN NUMBER)
IS


CURSOR c_hr_location 
IS 
    SELECT s.*
      FROM  MROI_HR_LOCATION_HDR_STG s, mroi_tt_trx_status trx
     WHERE s.INSTANCE_ID = P_INSTANCE_ID
       AND s.INSTANCE_ID = trx.INSTANCE_ID
       AND s.transaction_ID = trx.transaction_ID
       AND trx.status_code IN (MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_ERRED_STG, MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_ERRED_OIT_API)
     ORDER BY RECORD_HEADER_ID ASC;


CURSOR c_trans_error (p_transaction_id NUMBER)
IS
    select * from MROI_HR_LOCATION_ERR WHERE transaction_id = p_transaction_id;  
    

lv_error_msg VARCHAR2(4000);


l_counter NUMBER := 1;
BEGIN


    FOR r_hr_location IN c_hr_location LOOP
    
        lv_error_msg := NULL;
        IF l_counter = 1 THEN
            write_output ('Global,Name,Description,Address Style,Address Line1,Address Line2,Address Line3,City,State,Zip Code,Country,TimeZone,Ship To Site,Receiving Site,Office Site,Bill To Site,Internal Site,Inventory Organization,Error Message');    
        END IF;

        --get all error for transction
        FOR r_trans_error IN c_trans_error (r_hr_location.transaction_id) LOOP
            lv_error_msg := SUBSTR(lv_error_msg || MROI_TT_ERROR_UTIL_PKG.GET_ERROR_MSG(r_trans_error.ERROR_CODE,
                                                                                    MROI_TT_STATUS_UTIL_PKG.get_process_id('DEFAULT'),
                                                                                    r_trans_error.ERROR_FIELD,
                                                                                    r_trans_error.ERROR_ATTRIBUTE1, 
                                                                                    r_trans_error.ERROR_ATTRIBUTE2,
                                                                                    r_trans_error.ERROR_ATTRIBUTE3, 
                                                                                    r_trans_error.ERROR_ATTRIBUTE4,
                                                                                    r_trans_error.ORA_ERR_CODE,
                                                                                    r_trans_error.ORA_ERR_MSG) || ' '
                                    ,0,4000);
        END LOOP;
        
        write_output (
                        parse_string(r_hr_location.GLOBAL_FLAG) || ',' ||
                        parse_string(r_hr_location.LOCATION_NAME) || ',' ||           
                        parse_string(r_hr_location.LOCATION_DESCRIPTION) || ',' ||     
                        parse_string(r_hr_location.ADDRESS_STYLE) || ',' ||
                        parse_string(r_hr_location.ADDRESS_LINE1) || ',' ||       	
                        parse_string(r_hr_location.ADDRESS_LINE2) || ',' ||        	
                        parse_string(r_hr_location.ADDRESS_LINE3) || ',' ||        	
                        parse_string(r_hr_location.CITY) || ',' ||          		
                        parse_string(r_hr_location.STATE) || ',' ||     			
                        parse_string(r_hr_location.ZIPCODE) || ',' ||       		
                        parse_string(r_hr_location.COUNTRY) || ',' ||       		
                        parse_string(r_hr_location.TIMEZONE)   || ',' ||      		
                        parse_string(r_hr_location.SHIP_TO_SITE)  || ',' ||       	
                        parse_string(r_hr_location.RECEIVING_SITE)   || ',' ||      	
                        parse_string(r_hr_location.OFFICE_SITE)  || ',' ||      	
                        parse_string(r_hr_location.BILL_TO_SITE)  || ','  ||     	
                        parse_string(r_hr_location.INTERNAL_SITE)   || ','  ||    	
                        parse_string(r_hr_location.INVENTORY_ORGANIZATION)    || ','  ||   
                        parse_string(lv_error_msg)     
                    );
        l_counter := l_counter + 1;
    
    END LOOP;

END write_outfile;

--for high volume runs, conversion can be configured to run in parallel mode by creating a user profile option
--with user profile name matches that of the concurrent program name.  the user profile set at the site level will 
--drive the number of parallel process to run via concurrent manager.
FUNCTION get_parallel_count(p_concurrent_request_id IN NUMBER) RETURN NUMBER
IS

lv_parallel_count NUMBER := 1;
lv_concurrent_program_name VARCHAR2(4000);

BEGIN
                
    SELECT p.concurrent_program_name
      INTO lv_concurrent_program_name
      FROM fnd_concurrent_requests r, fnd_concurrent_programs p
     WHERE r.concurrent_program_id = p.concurrent_program_id
       AND request_id = p_concurrent_request_id;--;fnd_global.conc_request_id
                
    SELECT profile_option_value 
      INTO lv_parallel_count
      FROM fnd_profile_option_values fpov, fnd_profile_options fpo
     WHERE fpo.profile_option_id = fpov.profile_option_id
       AND level_id = 10001 --10001 Site, 10002 Application, 10003 Resp, 10004 User, 10005 Server
       AND fpo.profile_option_Name = lv_concurrent_program_name; --'MROI_HR_LOC_CNV';
             
      RETURN lv_parallel_count;
      
EXCEPTION WHEN OTHERS THEN
      write_log('Parallel Count Error: ' || SQLERRM);
      lv_parallel_count := 1;
      RETURN lv_parallel_count;
END get_parallel_count;


--when parallel mode is enabled, the workload load is divided evenly among each process.
--ie if the workload has 20K records and the number of specified parallel process is 5 then each process will pick up 4000 a piece
PROCEDURE spawn_parallel_process (P_MODE VARCHAR2, p_parent_request_id NUMBER, p_instance_id NUMBER , p_record_count NUMBER, p_parallel_count NUMBER)
IS

CURSOR c_hr_location 
IS 
    SELECT * 
    FROM  MROI_HR_LOCATION_HDR_STG
    WHERE instance_id = p_instance_id
    ORDER BY RECORD_HEADER_ID ASC;

lv_count_per_instance NUMBER := CEIL(p_record_count/p_parallel_count);
lv_concurrent_request_id NUMBER;
lv_user_id NUMBER; 
lv_responsibility_id NUMBER; 
lv_application_id NUMBER;
lv_min_index NUMBER := NULL;
lv_max_index NUMBER := NULL;
lv_temp_max_index NUMBER := NULL;
lv_counter NUMBER := 1;
lv_request_id NUMBER;
TYPE lv_numbers_table IS TABLE OF NUMBER;

lv_request_id_list lv_numbers_table := lv_numbers_table();

lc_phase VARCHAR2(4000);
lc_status VARCHAR2(4000);
lc_dev_phase VARCHAR2(4000);
lv_dev_status VARCHAR2(4000);
lv_message VARCHAR2(4000);
lv_req_return_status BOOLEAN;
lv_concurrent_program_name VARCHAR2(4000);

BEGIN
             gv_proc_name := 'spawn_parallel_process';
             
            --get Parent request info
            select c.requested_by, c.responsibility_id,c.responsibility_application_id,  p.concurrent_program_name
            into lv_user_id, lv_responsibility_id, lv_application_id, lv_concurrent_program_name
            from fnd_concurrent_requests c , fnd_concurrent_programs p
            where c.concurrent_program_id = p.concurrent_program_id
            AND c.request_id = p_parent_request_id;
         
            
            apps.fnd_global.apps_initialize(lv_user_id, lv_responsibility_id, lv_application_id);

            FOR r_hr_location IN c_hr_location LOOP
            
                lv_temp_max_index := r_hr_location.RECORD_HEADER_ID;
            
                IF lv_min_index IS NULL THEN
                    lv_min_index := r_hr_location.RECORD_HEADER_ID;
                END IF;
                
                IF lv_counter >= lv_count_per_instance THEN
                    lv_max_index := r_hr_location.RECORD_HEADER_ID;
                    
                    lv_request_id := fnd_request.submit_request (
                                                                    application => 'XXMRO', --'XXMRO',
                                                                    program => lv_concurrent_program_name, --'MROI_HR_LOC_CNV',
                                                                    description => 'Parent Process ID: ' || TO_CHAR(p_parent_request_id) || ' Min Index: ' || lv_min_index || ' Max Index: ' || lv_max_index  ,
                                                                    start_time => SYSDATE,
                                                                    sub_request => FALSE,
                                                                    argument1 => P_MODE,
                                                                    argument2 => '', --FileName
                                                                    argument3 => p_instance_id,
                                                                    argument4 => lv_min_index,
                                                                    argument5 => lv_max_index                 
                                                                );
                    IF lv_request_id != 0 THEN
                        lv_request_id_list.extend;
                        lv_request_id_list(lv_request_id_list.count) := lv_request_id;                        
                    END IF;
                    
                    write_log('Request ID: ' || lv_request_id || ' Min Max Counter: ' || TO_CHAR(lv_min_index) || '-' || TO_CHAR(lv_max_index));
                    
                    lv_counter := 1;
                    lv_min_index := NULL;
                    lv_max_index := NULL;
    
                ELSE
                    lv_counter := lv_counter + 1;
                END IF;
                
            END LOOP;
            
            IF lv_min_index IS NOT NULL AND lv_max_index IS NULL THEN
                lv_max_index := lv_temp_max_index;
                lv_request_id := fnd_request.submit_request (
                                                                    application => 'XXMRO',
                                                                    program => lv_concurrent_program_name, --'MROI_HR_LOC_CNV',
                                                                    description => 'Parent Process ID: ' || TO_CHAR(p_parent_request_id) || ' Min Index: ' || lv_min_index || ' Max Index: ' || lv_max_index  ,
                                                                    start_time => SYSDATE,
                                                                    sub_request => FALSE,
                                                                    argument1 => P_MODE,
                                                                    argument2 => '', --FileName
                                                                    argument3 => p_instance_id,
                                                                    argument4 => lv_min_index,
                                                                    argument5 => lv_max_index                 
                                                                );
                 IF lv_request_id != 0 THEN
                        lv_request_id_list.extend;
                        lv_request_id_list(lv_request_id_list.count) := lv_request_id;                        
                 END IF;                                                
                write_log('Request ID: ' || lv_request_id || ' Min Max Counter: ' || TO_CHAR(lv_min_index) || '-' || TO_CHAR(lv_max_index));
                
            END IF;
            
            COMMIT;
            
            --wait for all child processes to complete before returning.
            FOR i in 1 .. lv_request_id_list.COUNT
            LOOP
                lv_request_id := lv_request_id_list(i);
                
                LOOP
                    lv_req_return_status := fnd_concurrent.wait_for_request (   request_id => lv_request_id
                                                                               ,interval => 30 
                                                                               ,max_wait => NULL
                                                                               ,phase => lc_phase
                                                                               ,status => lc_status
                                                                               ,dev_phase => lc_dev_phase
                                                                               ,dev_status => lv_dev_status
                                                                               ,message => lv_message
                                                                            );
                
                EXIT WHEN UPPER(lc_phase) = 'COMPLETED' OR UPPER(lc_status) IN ('CANCELLED', 'ERROR', 'TERMINATED');
                END LOOP;
                
            END LOOP;            

END spawn_parallel_process;


 /* ***************************************************************************
 /*  Name:          stage_file_records 
 /*  Object Type:   Procedure
 /*  Description:   Procedure to read from external table and load into staging 
 /*                 table, then invoke MIMES
 /*
 /*  Calls:         MROI_TT_STATUS_UTIL_PKG
 /*  Source:        MROI_HR_LOCATION_CNV_PKG.pkb
 /*  Parameters:    p_instance_id - IN parameter for MIMES instance ID
 /*                 p_user_id - IN parameter for FND User ID
 /*                 p_x_total_txn_count - OUT parameter for number of records staged
 /*                 p_x_mroi_error_code - OUT parameter for error code
 /*                 p_x_mroi_error_msg - OUT parameter for error message
 /*                 p_x_mimes_error_code - OUT parameter for MIMES error code
 /*                 p_x_mimes_error_msg - OUT parameter for MIMES error message
 /*  Return values: N/A
 /*
 /*  Change History:
 /*  Date          Name                           Ver      Modification
 /*  -----------   ----------------------         ----     -------------
 /*  06/05/2019    Vinh Giang                     1.0      Initial Creation
 /******************************************************************************/	
PROCEDURE stage_file_records (
				p_instance_id IN NUMBER,
				p_user_id IN NUMBER,
				p_x_total_txn_count OUT NUMBER,
				p_x_mroi_error_code OUT NUMBER,
				p_x_mroi_error_msg OUT VARCHAR2,
				p_x_mimes_error_code OUT NUMBER,
				p_x_mimes_error_msg OUT VARCHAR2
				)
IS
	l_dbg_msg				VARCHAR2(2000);
	l_x_total_txn_count		NUMBER;
	l_x_mimes_error_code 	NUMBER := 0;
	l_x_mimes_error_msg 	VARCHAR2(2000);
	e_mimes_exception		EXCEPTION;
  
BEGIN
		l_dbg_msg := '200.01 Inserting records into MROI_HR_LOCATION_HDR_STG table.';
        write_log('Inserting records into MROI_HR_LOCATION_HDR_STG table.');

        --pull data from external table into staging table
        INSERT INTO MROI_HR_LOCATION_HDR_STG 
                    (RECORD_HEADER_ID
                    ,INSTANCE_ID
                    ,TRANSACTION_ID
                    ,GLOBAL_FLAG                	
                    ,LOCATION_NAME                  
                    ,LOCATION_DESCRIPTION           
                    ,ADDRESS_STYLE               	
                    ,ADDRESS_LINE1         			
                    ,ADDRESS_LINE2         			
                    ,ADDRESS_LINE3         			
                    ,CITY           				
                    ,STATE      					
                    ,ZIPCODE        				
                    ,COUNTRY        				
                    ,TIMEZONE        				
                    ,SHIP_TO_SITE        			
                    ,RECEIVING_SITE        			
                    ,OFFICE_SITE        			
                    ,BILL_TO_SITE        			
                    ,INTERNAL_SITE        			
                    ,INVENTORY_ORGANIZATION	
                    ,CREATION_DATE    
                    ,LAST_UPDATE_DATE 
                    ,CREATED_BY       
                    ,LAST_UPDATED_BY
                    )
                    SELECT   MROI_HR_LOCATION_HDR_STG_S.NEXTVAL
                            ,p_instance_id
                            ,NULL --transaction_id
                            ,GLOBAL_FLAG                	
                            ,LOCATION_NAME                  
                            ,LOCATION_DESCRIPTION           
                            ,ADDRESS_STYLE               	
                            ,ADDRESS_LINE1         			
                            ,ADDRESS_LINE2         			
                            ,ADDRESS_LINE3         			
                            ,CITY           				
                            ,STATE      					
                            ,ZIPCODE        				
                            ,COUNTRY        				
                            ,TIMEZONE        				
                            ,SHIP_TO_SITE        			
                            ,RECEIVING_SITE        			
                            ,OFFICE_SITE        			
                            ,BILL_TO_SITE        			
                            ,INTERNAL_SITE        			
                            ,INVENTORY_ORGANIZATION		
                            ,SYSDATE
                            ,SYSDATE
                            ,-1
                            ,-1                            
                    FROM MROI_HR_LOCATION_HDR_STG_EXT;
                   
        p_x_total_txn_count := SQL%ROWCOUNT;
        write_log('Record Count: ' || TO_CHAR(p_x_total_txn_count));
        
        COMMIT;
        
       -- call MIMES API to insert generated collection of transaction ids into MROI_TT_TRX_STATUS table
       l_dbg_msg := '002. Calling MROI_TT_STATUS_UTIL_PKG.initialize_trx_batch. ';                                        
       MROI_TT_STATUS_UTIL_PKG.initialize_trx_batch(p_instance_id      => p_instance_id
                                                   ,p_user_id          => p_user_id
                                                   ,p_x_mimes_err_code => l_x_mimes_error_code
                                                   ,p_x_mimes_err_msg  => l_x_mimes_error_msg);
                                                        
       IF l_x_mimes_error_code = 1 THEN           
          RAISE e_mimes_exception;                                             
       END IF;
        
       COMMIT; 	

    p_x_mroi_error_code := 0;
	p_x_mroi_error_msg := NULL;
	p_x_mimes_error_code := l_x_mimes_error_code;
	p_x_mimes_error_msg := l_x_mimes_error_msg;
EXCEPTION
	WHEN e_mimes_exception THEN
		p_x_mimes_error_code := l_x_mimes_error_code;
		p_x_mimes_error_msg := 'MIMES exception occurred when '||l_dbg_msg||' - '||l_x_mimes_error_msg;
	WHEN OTHERS THEN
		p_x_mroi_error_code := 1;
		p_x_mroi_error_msg := 'Other exception occurred when '||l_dbg_msg||' - '||SQLERRM;
END stage_file_records;

FUNCTION get_Instance_status (p_instance_id NUMBER) RETURN STRING
IS
l_status VARCHAR2(100);
BEGIN

    select status_code into l_status from MROI_TT_INSTANCE_STATUS where instance_id= p_instance_id; 
    RETURN l_status;
    
EXCEPTION WHEN OTHERS THEN
    RETURN l_status;
END get_Instance_status;

PROCEDURE print_transaction_status (p_instance_id NUMBER)
IS

CURSOR c_hr_location 
IS 
    SELECT s.location_name, trx.transaction_id, trx.status_code 
      FROM  MROI_HR_LOCATION_HDR_STG s, mroi_tt_trx_status trx
     WHERE s.INSTANCE_ID = P_INSTANCE_ID
       AND s.INSTANCE_ID = trx.INSTANCE_ID
       AND s.transaction_ID = trx.transaction_ID
       --AND RECORD_HEADER_ID BETWEEN NVL(P_MIN_INDEX,RECORD_HEADER_ID - 1) AND  NVL(P_MAX_INDEX,RECORD_HEADER_ID + 1)
     ORDER BY RECORD_HEADER_ID ASC;

CURSOR c_trans_error (p_transaction_id NUMBER)
IS
    select * from MROI_HR_LOCATION_ERR WHERE transaction_id = p_transaction_id;  
    
l_status VARCHAR2(100);
lv_error_msg VARCHAR2(4000);
BEGIN

   write_log (RPAD('-', 120, '-'));
   write_log (RPAD('Location Name', 30, ' ') || RPAD('Transaction ID',30, ' ' ) || RPAD('Status Code',30,' ') || RPAD('Error',30,' ') );
   write_log (RPAD('-', 120, '-'));
   
   FOR r_hr_location IN c_hr_location LOOP
        lv_error_msg := '';
        
        --get all error for transction
        FOR r_trans_error IN c_trans_error (r_hr_location.transaction_id) LOOP
            lv_error_msg := SUBSTR(lv_error_msg || MROI_TT_ERROR_UTIL_PKG.GET_ERROR_MSG(r_trans_error.ERROR_CODE,
                                                                                    MROI_TT_STATUS_UTIL_PKG.get_process_id('DEFAULT'),
                                                                                    r_trans_error.ERROR_FIELD,
                                                                                    r_trans_error.ERROR_ATTRIBUTE1, 
                                                                                    r_trans_error.ERROR_ATTRIBUTE2,
                                                                                    r_trans_error.ERROR_ATTRIBUTE3, 
                                                                                    r_trans_error.ERROR_ATTRIBUTE4,
                                                                                    r_trans_error.ORA_ERR_CODE,
                                                                                    r_trans_error.ORA_ERR_MSG) || ' '
                                    ,0,4000);
        END LOOP;
       -- write_log(r_hr_location.location_name || ',' || r_hr_location.transaction_id || r_hr_location.status_code );
       write_log(RPAD(NVL(r_hr_location.location_name,' '), 30,' ') || RPAD(TO_CHAR(r_hr_location.transaction_id),30,' ') || RPAD(r_hr_location.status_code,30,' ') || lv_error_msg);
   
   END LOOP;
   write_log ('');
    
EXCEPTION WHEN OTHERS THEN
    write_log('print_transaction_status: ' || SQLERRM);
END print_transaction_status;

PROCEDURE validate_records (    P_INSTANCE_ID IN NUMBER,
                                P_MIN_INDEX IN NUMBER,
                                P_MAX_INDEX IN NUMBER,
                                p_x_error_count OUT NUMBER,
                                p_x_mroi_error_code OUT NUMBER,
                                p_x_mroi_error_msg OUT VARCHAR2,
                                p_x_mimes_error_code OUT NUMBER,
                                p_x_mimes_error_msg OUT VARCHAR2                                
                                )
IS

    
	CURSOR c_invalid_values (p_c_instance_id NUMBER) IS
        SELECT MROI_HR_LOCATION_ERR_S.nextval AS error_id, rec.instance_id, rec.transaction_id, rec.record_level_id, rec.record_level_type, rec.error_field, rec.error_code, rec.error_attribute1, rec.error_attribute2, rec.error_attribute3, rec.error_attribute4
        FROM (
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'GLOBAL_FLAG' as error_field, 13000 as error_code,
            '' as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_HR_LOCATION_HDR_STG a
            WHERE a.GLOBAL_FLAG IS NULL
            AND RECORD_HEADER_ID BETWEEN NVL(P_MIN_INDEX,RECORD_HEADER_ID - 1) AND  NVL(P_MAX_INDEX,RECORD_HEADER_ID + 1)
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'LOCATION_NAME' as error_field, 13000 as error_code,
            '' as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_HR_LOCATION_HDR_STG a
            WHERE a.LOCATION_NAME IS NULL
            AND RECORD_HEADER_ID BETWEEN NVL(P_MIN_INDEX,RECORD_HEADER_ID - 1) AND  NVL(P_MAX_INDEX,RECORD_HEADER_ID + 1)
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'LOCATION_DESCRIPTION' as error_field, 13000 as error_code,
            '' as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_HR_LOCATION_HDR_STG a
            WHERE a.LOCATION_DESCRIPTION IS NULL
            AND RECORD_HEADER_ID BETWEEN NVL(P_MIN_INDEX,RECORD_HEADER_ID - 1) AND  NVL(P_MAX_INDEX,RECORD_HEADER_ID + 1)
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'ADDRESS_STYLE' as error_field, 13000 as error_code,
            '' as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_HR_LOCATION_HDR_STG a
            WHERE a.ADDRESS_STYLE IS NULL
            AND RECORD_HEADER_ID BETWEEN NVL(P_MIN_INDEX,RECORD_HEADER_ID - 1) AND  NVL(P_MAX_INDEX,RECORD_HEADER_ID + 1)
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'ADDRESS_LINE1' as error_field, 13000 as error_code,
            '' as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_HR_LOCATION_HDR_STG a
            WHERE a.ADDRESS_LINE1 IS NULL
            AND RECORD_HEADER_ID BETWEEN NVL(P_MIN_INDEX,RECORD_HEADER_ID - 1) AND  NVL(P_MAX_INDEX,RECORD_HEADER_ID + 1)
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'CITY' as error_field, 13000 as error_code,
            '' as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_HR_LOCATION_HDR_STG a
            WHERE a.CITY IS NULL
            AND RECORD_HEADER_ID BETWEEN NVL(P_MIN_INDEX,RECORD_HEADER_ID - 1) AND  NVL(P_MAX_INDEX,RECORD_HEADER_ID + 1)
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'STATE' as error_field, 13000 as error_code,
            '' as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_HR_LOCATION_HDR_STG a
            WHERE a.STATE IS NULL
            AND RECORD_HEADER_ID BETWEEN NVL(P_MIN_INDEX,RECORD_HEADER_ID - 1) AND  NVL(P_MAX_INDEX,RECORD_HEADER_ID + 1)
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'ZIPCODE' as error_field, 13000 as error_code,
            '' as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_HR_LOCATION_HDR_STG a
            WHERE a.ZIPCODE IS NULL
            AND RECORD_HEADER_ID BETWEEN NVL(P_MIN_INDEX,RECORD_HEADER_ID - 1) AND  NVL(P_MAX_INDEX,RECORD_HEADER_ID + 1)
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'COUNTRY' as error_field, 13000 as error_code,
            '' as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_HR_LOCATION_HDR_STG a
            WHERE a.COUNTRY IS NULL
            AND RECORD_HEADER_ID BETWEEN NVL(P_MIN_INDEX,RECORD_HEADER_ID - 1) AND  NVL(P_MAX_INDEX,RECORD_HEADER_ID + 1)
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'TIMEZONE' as error_field, 13000 as error_code,
            '' as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_HR_LOCATION_HDR_STG a
            WHERE a.TIMEZONE IS NULL
            AND RECORD_HEADER_ID BETWEEN NVL(P_MIN_INDEX,RECORD_HEADER_ID - 1) AND  NVL(P_MAX_INDEX,RECORD_HEADER_ID + 1)
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'INVENTORY_ORGANIZATION' as error_field, 13000 as error_code,
            '' as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_HR_LOCATION_HDR_STG a
            WHERE a.INVENTORY_ORGANIZATION IS NULL
            AND RECORD_HEADER_ID BETWEEN NVL(P_MIN_INDEX,RECORD_HEADER_ID - 1) AND  NVL(P_MAX_INDEX,RECORD_HEADER_ID + 1)
            UNION ALL

            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'ADDRESS_STYLE' as error_field, 15001 as error_code,
            'ADDRESS_STYLE' as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_HR_LOCATION_HDR_STG a
            WHERE a.ADDRESS_STYLE IS NOT NULL
            AND RECORD_HEADER_ID BETWEEN NVL(P_MIN_INDEX,RECORD_HEADER_ID - 1) AND  NVL(P_MAX_INDEX,RECORD_HEADER_ID + 1)
            AND NOT EXISTS (select 'X' 
                            from FND_DESCR_FLEX_CONTEXTS_VL 
                            WHERE DESCRIPTIVE_FLEXFIELD_NAME='Address Location'
                            AND DESCRIPTIVE_FLEX_CONTEXT_NAME = A.address_style
                            AND enabled_flag ='Y' )
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'STATE' as error_field, 15001 as error_code,
            'STATE' as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_HR_LOCATION_HDR_STG a
            WHERE a.STATE IS NOT NULL
            AND RECORD_HEADER_ID BETWEEN NVL(P_MIN_INDEX,RECORD_HEADER_ID - 1) AND  NVL(P_MAX_INDEX,RECORD_HEADER_ID + 1)
            AND NOT EXISTS (SELECT 'X'
                            FROM  FND_LOOKUP_VALUES_VL 
                            WHERE lookup_type = 'US_STATE'
                            AND LOOKUP_CODE = a.state )
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'COUNTRY' as error_field, 15001 as error_code,
            'COUNTRY' as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_HR_LOCATION_HDR_STG a
            WHERE a.COUNTRY IS NOT NULL
            AND RECORD_HEADER_ID BETWEEN NVL(P_MIN_INDEX,RECORD_HEADER_ID - 1) AND  NVL(P_MAX_INDEX,RECORD_HEADER_ID + 1)
            AND NOT EXISTS (SELECT 'X'
                            FROM  fnd_territories_tl 
                            WHERE territory_short_name= a.country )
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'INVENTORY_ORGANIZATION' as error_field, 15001 as error_code,
            'INVENTORY_ORGANIZATION' as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_HR_LOCATION_HDR_STG a
            WHERE a.INVENTORY_ORGANIZATION IS NOT NULL
            AND RECORD_HEADER_ID BETWEEN NVL(P_MIN_INDEX,RECORD_HEADER_ID - 1) AND  NVL(P_MAX_INDEX,RECORD_HEADER_ID + 1)
            AND NOT EXISTS (SELECT 'X'
                            FROM  mtl_parameters 
                            WHERE organization_code = a.inventory_organization )
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'TIMEZONE' as error_field, 15001 as error_code,
            'TIMEZONE' as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_HR_LOCATION_HDR_STG a
            WHERE a.TIMEZONE IS NOT NULL
            AND RECORD_HEADER_ID BETWEEN NVL(P_MIN_INDEX,RECORD_HEADER_ID - 1) AND  NVL(P_MAX_INDEX,RECORD_HEADER_ID + 1)
            AND NOT EXISTS (SELECT 'X'
                            FROM  fnd_timezones_tl 
                            WHERE  name = a.TIMEZONE )            
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'LOCATION_NAME' as error_field, 16004 as error_code,
            'LOCATION_NAME' as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_HR_LOCATION_HDR_STG a
            WHERE a.LOCATION_NAME IS NOT NULL
            AND RECORD_HEADER_ID BETWEEN NVL(P_MIN_INDEX,RECORD_HEADER_ID - 1) AND  NVL(P_MAX_INDEX,RECORD_HEADER_ID + 1)
            AND EXISTS (SELECT 'X'
                            FROM  hr_locations_all 
                            WHERE location_code = a.location_name )

        ) rec
        WHERE INSTANCE_ID = p_c_instance_id;
        
     
--TYPE loc_tab IS TABLE OF  loc_tab_row;
--TYPE error_tab IS TABLE OF error_tab_row;
--lv_loc_collection loc_tab;
lv_error_collection				MROI_TT_ERROR_UTIL_PKG.t_err_tbl := MROI_TT_ERROR_UTIL_PKG.t_err_tbl();
--lv_error_collection error_tab := error_tab();
l_user_id NUMBER := MROI_UTIL_PKG.get_user_id('CONVERSION'); 
lv_count NUMBER;
l_dbg_msg VARCHAR2(400);
l_t_err_tbl				MROI_TT_ERROR_UTIL_PKG.t_err_tbl := MROI_TT_ERROR_UTIL_PKG.t_err_tbl();
l_x_mimes_error_code 	NUMBER := 0;
l_x_mimes_error_msg 	VARCHAR2(2000);
e_mimes_exception EXCEPTION;
lv_error_collection_count NUMBER;
l_t_trx_status_tbl		MROI_TT_STATUS_UTIL_PKG.t_trx_status_tbl := MROI_TT_STATUS_UTIL_PKG.t_trx_status_tbl();

BEGIN

    
    gv_proc_name := 'validate_records';
  
  
    l_dbg_msg := '300.00 Opening cursor FOR LOOP for c_invalid_values using Instance ID: '||p_instance_id;
    --write_log('Opening cursor FOR LOOP for c_invalid_values using Instance ID: '||p_instance_id);
	FOR r_invalid_values IN c_invalid_values(p_instance_id) LOOP
        -- Assign error to the error table variable
        l_t_err_tbl.EXTEND;
        l_t_err_tbl(l_t_err_tbl.COUNT).ERROR_ID := r_invalid_values.ERROR_ID;
        l_t_err_tbl(l_t_err_tbl.COUNT).TRANSACTION_ID := r_invalid_values.TRANSACTION_ID;
        l_t_err_tbl(l_t_err_tbl.COUNT).RECORD_LEVEL_ID := r_invalid_values.RECORD_LEVEL_ID;
        l_t_err_tbl(l_t_err_tbl.COUNT).RECORD_LEVEL_TYPE := r_invalid_values.RECORD_LEVEL_TYPE; 
        l_t_err_tbl(l_t_err_tbl.COUNT).ERROR_FIELD := r_invalid_values.ERROR_FIELD; 
        l_t_err_tbl(l_t_err_tbl.COUNT).ERROR_CODE := r_invalid_values.ERROR_CODE;
        l_t_err_tbl(l_t_err_tbl.COUNT).ERROR_ATTRIBUTE1 := r_invalid_values.ERROR_ATTRIBUTE1;
        l_t_err_tbl(l_t_err_tbl.COUNT).ERROR_ATTRIBUTE2 := r_invalid_values.ERROR_ATTRIBUTE2;
        l_t_err_tbl(l_t_err_tbl.COUNT).ERROR_ATTRIBUTE3 := r_invalid_values.ERROR_ATTRIBUTE3;
        l_t_err_tbl(l_t_err_tbl.COUNT).ERROR_ATTRIBUTE4 := r_invalid_values.ERROR_ATTRIBUTE4;
        
        -- Assign error status code to the transaction status table variable
        l_t_trx_status_tbl.EXTEND; 
        l_t_trx_status_tbl(l_t_trx_status_tbl.COUNT).STATUS_CODE := MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_ERRED_STG;
        l_t_trx_status_tbl(l_t_trx_status_tbl.COUNT).TRANSACTION_ID := r_invalid_values.TRANSACTION_ID;    
	END LOOP; -- FOR r_invalid_values IN c_invalid_values(p_instance_id) LOOP

	-- Calls MIMES APIs if there are errors found
    IF l_t_err_tbl.COUNT > 0 THEN
   
		-- Call MIMES API to insert collection of erred records into MROI_HR_LOCATION_ERR table
		l_dbg_msg := '300.01 Calling MROI_TT_ERROR_UTIL_PKG.log_error_batch for Instance ID: '||p_instance_id;
		--write_log('Calling MROI_TT_ERROR_UTIL_PKG.log_error_batch for Instance ID: '||p_instance_id);		
		MROI_TT_ERROR_UTIL_PKG.log_error_batch(p_instance_id       => p_instance_id
											  ,p_user_id           => l_user_id
											  ,p_t_err_tbl         => l_t_err_tbl 
											  ,p_x_mimes_err_code  => l_x_mimes_error_code
											  ,p_x_mimes_err_msg   => l_x_mimes_error_msg);
										  
		IF l_x_mimes_error_code = 1 THEN                                            
			RAISE e_mimes_exception;                                             
		END IF;
		  
		l_dbg_msg := '300.02  Calling MROI_TT_STATUS_UTIL_PKG.update_trx_status_batch for Instance ID: '||p_instance_id;
		--write_log('Calling MROI_TT_STATUS_UTIL_PKG.update_trx_status_batch for Instance ID: '||p_instance_id);		
		MROI_TT_STATUS_UTIL_PKG.update_trx_status_batch(p_trx_status_tbl     => l_t_trx_status_tbl  
														,p_user_id           => l_user_id                                                               
														,p_x_mimes_err_code  => l_x_mimes_error_code
														,p_x_mimes_err_msg   => l_x_mimes_error_msg);
		 IF l_x_mimes_error_code = 1 THEN                                            
			RAISE e_mimes_exception;                                             
		 END IF;
		                  
    END IF; -- IF l_t_err_tbl.COUNT > 0
    
	l_dbg_msg := '300.03  Clearing table type variables.';
	--write_log('Clearing table type variables.');
    l_t_err_tbl.DELETE; 
    l_t_trx_status_tbl.DELETE;      
      
    l_dbg_msg := '300.04 Getting validation transaction record error count for Instance ID: '||p_instance_id;
	--write_log('Getting validation transaction record error count for Instance ID: '||p_instance_id);
    SELECT COUNT(*)
      INTO p_x_error_count
      FROM MROI_TT_TRX_STATUS   
     WHERE STATUS_CODE = MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_ERRED_STG
       AND INSTANCE_ID =  p_instance_id;          
  
    COMMIT;
    
END validate_records;



PROCEDURE process_records (     P_INSTANCE_ID IN NUMBER,
                                P_MIN_INDEX IN NUMBER,
                                P_MAX_INDEX IN NUMBER,
                                p_x_error_count OUT NUMBER,
                                p_x_mroi_error_code OUT NUMBER,
                                p_x_mroi_error_msg OUT VARCHAR2,
                                p_x_mimes_error_code OUT NUMBER,
                                p_x_mimes_error_msg OUT VARCHAR2                                
                                )
IS

    

CURSOR c_hr_location 
IS 
    SELECT a.*
      FROM MROI_HR_LOCATION_HDR_STG a, mroi_tt_trx_status t
     WHERE a.instance_id =P_INSTANCE_ID
       AND a.transaction_id = t.transaction_id
       AND t.status_code = MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_LOADED_STG
       AND RECORD_HEADER_ID BETWEEN NVL(P_MIN_INDEX,RECORD_HEADER_ID - 1) AND  NVL(P_MAX_INDEX,RECORD_HEADER_ID + 1);
                
TYPE loc_tab IS TABLE OF  loc_tab_row;
--TYPE error_tab IS TABLE OF error_tab_row;
lv_loc_collection loc_tab;        



l_validate			BOOLEAN := FALSE;
l_name				HR_LOCATIONS_ALL.LOCATION_CODE%TYPE := 'AFS Chicago Location Name';
l_description		HR_LOCATIONS_ALL.DESCRIPTION%TYPE := 'AFS Chicago Location Description';
l_address_style		HR_LOCATIONS_ALL.STYLE%TYPE := 'US_GLB';
l_addr_line1		HR_LOCATIONS_ALL.ADDRESS_LINE_1%TYPE := '161 North Clark Street';
l_addr_line2		HR_LOCATIONS_ALL.ADDRESS_LINE_2%TYPE := '16th Floor'; -- Optional
l_addr_line3		HR_LOCATIONS_ALL.ADDRESS_LINE_3%TYPE; -- Optional
l_city				HR_LOCATIONS_ALL.TOWN_OR_CITY%TYPE := 'Chicago';
l_state				HR_LOCATIONS_ALL.REGION_2%TYPE := 'IL'; -- Add validation
l_zip_code			HR_LOCATIONS_ALL.POSTAL_CODE%TYPE := '60601';
l_country			HR_LOCATIONS_ALL.COUNTRY%TYPE := 'US'; -- Add validation
l_time_zone			HR_LOCATIONS_ALL.TIMEZONE_CODE%TYPE := 'America/Chicago'; -- Add validation
l_ship_to_site		HR_LOCATIONS_ALL.SHIP_TO_SITE_FLAG%TYPE := 'Y'; -- Optional
l_receiving_site	HR_LOCATIONS_ALL.RECEIVING_SITE_FLAG%TYPE := 'Y'; -- Optional
l_office_site		HR_LOCATIONS_ALL.OFFICE_SITE_FLAG%TYPE := 'Y'; -- Optional
l_bill_to_site		HR_LOCATIONS_ALL.BILL_TO_SITE_FLAG%TYPE := 'Y'; -- Optional
l_internal_site		HR_LOCATIONS_ALL.IN_ORGANIZATION_FLAG%TYPE := 'Y'; -- Optional
l_inv_org_code		MTL_PARAMETERS.ORGANIZATION_CODE%TYPE := 'MST';
l_inv_org_id		MTL_PARAMETERS.ORGANIZATION_ID%TYPE;

l_location_id		HR_LOCATIONS_ALL.LOCATION_ID%TYPE;
l_ovn				HR_LOCATIONS_ALL.OBJECT_VERSION_NUMBER%TYPE;
l_t_trx_status_tbl		MROI_TT_STATUS_UTIL_PKG.t_trx_status_tbl := MROI_TT_STATUS_UTIL_PKG.t_trx_status_tbl();
l_e_trx_status_tbl		MROI_TT_STATUS_UTIL_PKG.t_trx_status_tbl := MROI_TT_STATUS_UTIL_PKG.t_trx_status_tbl();
l_loaded_ebs_trx_status_tbl		MROI_TT_STATUS_UTIL_PKG.t_trx_status_tbl := MROI_TT_STATUS_UTIL_PKG.t_trx_status_tbl();
l_user_id NUMBER := MROI_UTIL_PKG.get_user_id('CONVERSION'); 
l_x_mimes_error_code 	NUMBER := 0;
l_x_mimes_error_msg 	VARCHAR2(2000);

BEGIN            


    gv_proc_name := 'process_records';
        
    OPEN c_hr_location;
    LOOP
        FETCH c_hr_location BULK COLLECT INTO lv_loc_collection LIMIT 1000;
        
        FOR i in 1 .. lv_loc_collection.COUNT
        LOOP
            write_log('***Location: ' || lv_loc_collection(i).location_name || ' Transaction ID: '|| lv_loc_collection(i).TRANSACTION_ID  );
        
            --data transformation needed
            SELECT DECODE( UPPER(lv_loc_collection(i).bill_to_site),'CHECKED','Y', 'N') INTO l_bill_to_site FROM DUAL;
            SELECT DECODE( UPPER(lv_loc_collection(i).internal_site),'CHECKED','Y', 'N') INTO l_internal_site FROM DUAL;
            SELECT DECODE( UPPER(lv_loc_collection(i).office_site),'CHECKED','Y', 'N') INTO l_office_site FROM DUAL;
            SELECT DECODE( UPPER(lv_loc_collection(i).receiving_site),'CHECKED','Y', 'N') INTO l_receiving_site FROM DUAL;
            SELECT DECODE( UPPER(lv_loc_collection(i).ship_to_site),'CHECKED','Y', 'N') INTO l_ship_to_site FROM DUAL;
            
            --get address style
            BEGIN
                SELECT descriptive_flex_context_code
                  INTO l_address_style
                  FROM FND_DESCR_FLEX_CONTEXTS_VL 
                 WHERE DESCRIPTIVE_FLEXFIELD_NAME='Address Location'
                            AND DESCRIPTIVE_FLEX_CONTEXT_NAME = lv_loc_collection(i).address_style
                            AND enabled_flag ='Y';                        
            EXCEPTION WHEN OTHERS THEN
                l_address_style := NULL;
            END;

            --get timezone
            BEGIN            
                SELECT timezone_code
                  INTO l_time_zone
                  FROM fnd_timezones_tl 
                 WHERE name = lv_loc_collection(i).timezone;            
            EXCEPTION WHEN OTHERS THEN
                l_time_zone := NULL;
            END;

            --get country
            BEGIN            
                SELECT territory_code
                  INTO l_country
                  FROM fnd_territories_tl 
                 WHERE territory_short_name = lv_loc_collection(i).country;           
            EXCEPTION WHEN OTHERS THEN
                l_country := NULL;
            END;            

            --get inv org
            BEGIN            
                SELECT mp.organization_id
                 INTO l_inv_org_id
                  FROM mtl_parameters mp
                WHERE mp.organization_code = lv_loc_collection(i).INVENTORY_ORGANIZATION;
            EXCEPTION WHEN OTHERS THEN
                l_inv_org_id := NULL;
            END;            
            /*
            write_log('l_bill_to_site: ' || lv_loc_collection(i).bill_to_site || ' > ' || l_bill_to_site );
            write_log('l_internal_site: ' || lv_loc_collection(i).internal_site || ' > ' || l_internal_site );
            write_log('l_office_site: ' || lv_loc_collection(i).office_site || ' > ' || l_office_site );
            write_log('l_receiving_site: ' || lv_loc_collection(i).receiving_site || ' > ' || l_receiving_site );
            write_log('l_ship_to_site: ' || lv_loc_collection(i).ship_to_site || ' > ' || l_ship_to_site );
            write_log('l_address_style: ' || lv_loc_collection(i).address_style || ' > ' || l_address_style );          
            write_log('l_time_zone: ' || lv_loc_collection(i).timezone || ' > ' || l_time_zone );          
            write_log('l_country: ' || lv_loc_collection(i).country || ' > ' || l_country );          
            write_log('l_inv_org_id: ' || lv_loc_collection(i).INVENTORY_ORGANIZATION || ' > ' || l_inv_org_id );          
            */
            
            BEGIN
                -- Need to confirm if Ship-to Location ID should be used
                HR_LOCATION_API.create_location
                ( p_validate                       => l_validate, --IN  BOOLEAN   DEFAULT false
                  p_effective_date                 => TRUNC(sysdate), --IN  DATE
                  p_language_code                  => hr_api.userenv_lang, --IN  VARCHAR2  DEFAULT hr_api.userenv_lang
                  p_location_code                  => lv_loc_collection(i).location_name,--l_name, --IN  VARCHAR2
                  p_description                    => lv_loc_collection(i).location_description,--l_description, --IN  VARCHAR2  DEFAULT NULL
                  p_timezone_code                  => l_time_zone, --IN  VARCHAR2  DEFAULT NULL
                  p_tp_header_id                   => NULL, --IN  NUMBER    DEFAULT NULL
                  p_ece_tp_location_code           => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_address_line_1                 => lv_loc_collection(i).address_line1, --l_addr_line1, --IN  VARCHAR2  DEFAULT NULL
                  p_address_line_2                 => lv_loc_collection(i).address_line2, --l_addr_line2, --IN  VARCHAR2  DEFAULT NULL
                  p_address_line_3                 => lv_loc_collection(i).address_line3, --l_addr_line3, --IN  VARCHAR2  DEFAULT NULL
                  p_bill_to_site_flag              => l_bill_to_site, --IN  VARCHAR2  DEFAULT 'Y'
                  p_country                        => l_country, --IN  VARCHAR2  DEFAULT NULL
                  p_designated_receiver_id         => NULL, --IN  NUMBER    DEFAULT NULL
                  p_in_organization_flag           => l_internal_site, --IN  VARCHAR2  DEFAULT 'Y'
                  p_inactive_date                  => NULL, --IN  DATE      DEFAULT NULL
                  p_operating_unit_id              => NULL, --IN  NUMBER    DEFAULT NULL
                  p_inventory_organization_id      => l_inv_org_id, --IN  NUMBER    DEFAULT NULL
                  p_office_site_flag               => l_office_site, --IN  VARCHAR2  DEFAULT 'Y'
                  p_postal_code                    => lv_loc_collection(i).zipcode, --l_zip_code, --IN  VARCHAR2  DEFAULT NULL
                  p_receiving_site_flag            => l_receiving_site, --IN  VARCHAR2  DEFAULT 'Y'
                  p_region_1                       => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_region_2                       => lv_loc_collection(i).state, --l_state, --IN  VARCHAR2  DEFAULT NULL
                  p_region_3                       => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_ship_to_location_id            => NULL, --IN  NUMBER    DEFAULT NULL
                  p_ship_to_site_flag              => l_ship_to_site, --IN  VARCHAR2  DEFAULT 'Y'
                  p_style                          => l_address_style, --IN  VARCHAR2  DEFAULT NULL
                  p_tax_name                       => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_telephone_number_1             => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_telephone_number_2             => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_telephone_number_3             => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_town_or_city                   => lv_loc_collection(i).city, --l_city, --IN  VARCHAR2  DEFAULT NULL
                  p_loc_information13              => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_loc_information14              => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_loc_information15              => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_loc_information16              => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_loc_information17              => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_loc_information18              => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_loc_information19              => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_loc_information20              => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_attribute_category             => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_attribute1                     => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_attribute2                     => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_attribute3                     => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_attribute4                     => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_attribute5                     => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_attribute6                     => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_attribute7                     => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_attribute8                     => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_attribute9                     => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_attribute10                    => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_attribute11                    => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_attribute12                    => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_attribute13                    => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_attribute14                    => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_attribute15                    => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_attribute16                    => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_attribute17                    => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_attribute18                    => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_attribute19                    => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_attribute20                    => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_global_attribute_category      => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_global_attribute1              => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_global_attribute2              => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_global_attribute3              => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_global_attribute4              => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_global_attribute5              => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_global_attribute6              => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_global_attribute7              => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_global_attribute8              => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_global_attribute9              => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_global_attribute10             => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_global_attribute11             => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_global_attribute12             => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_global_attribute13             => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_global_attribute14             => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_global_attribute15             => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_global_attribute16             => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_global_attribute17             => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_global_attribute18             => NULL, --N  VARCHAR2  DEFAULT NULL
                  p_global_attribute19             => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_global_attribute20             => NULL, --IN  VARCHAR2  DEFAULT NULL
                  p_business_group_id              => NULL, --IN  NUMBER    DEFAULT NULL
                  p_location_id                    => l_location_id, --OUT NOCOPY NUMBER
                  p_object_version_number          => l_ovn --OUT NOCOPY NUMBER
                  );
              
                write_log('Location ID returned: '||l_location_id);
                write_log('Object Version Number returned: '|| l_ovn);

                -- Assign error status code to the transaction status table variable
                l_t_trx_status_tbl.EXTEND; 
                l_t_trx_status_tbl(l_t_trx_status_tbl.COUNT).STATUS_CODE := MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_LOADED_OIT_API;  
                l_t_trx_status_tbl(l_t_trx_status_tbl.COUNT).TRANSACTION_ID := lv_loc_collection(i).TRANSACTION_ID;    

                l_loaded_ebs_trx_status_tbl.EXTEND; 
                l_loaded_ebs_trx_status_tbl(l_loaded_ebs_trx_status_tbl.COUNT).STATUS_CODE := MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_LOADED_EBS;  
                l_loaded_ebs_trx_status_tbl(l_loaded_ebs_trx_status_tbl.COUNT).TRANSACTION_ID := lv_loc_collection(i).TRANSACTION_ID;    
                
            EXCEPTION WHEN OTHERS THEN
                -- Assign error status code to the transaction status table variable
                l_e_trx_status_tbl.EXTEND; 
                l_e_trx_status_tbl(l_e_trx_status_tbl.COUNT).STATUS_CODE := MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_ERRED_OIT_API;  --G_TRXCODE_LOADED_EBS
                l_e_trx_status_tbl(l_e_trx_status_tbl.COUNT).TRANSACTION_ID := lv_loc_collection(i).TRANSACTION_ID;    

                write_log('Error calling HR_LOCATION_API.create_location API');
                write_log('Error Message Returned: '||SUBSTR(SQLERRM,1,500));
            END;
            
            
            
        END LOOP;
              
            
    EXIT WHEN c_hr_location%NOTFOUND;
    END LOOP;


    --l_dbg_msg := '300.02  Calling MROI_TT_STATUS_UTIL_PKG.update_trx_status_batch for Instance ID: '||p_instance_id;
    IF l_t_trx_status_tbl.COUNT > 0 THEN
        MROI_TT_STATUS_UTIL_PKG.update_trx_status_batch(p_trx_status_tbl     => l_t_trx_status_tbl  
                                                            ,p_user_id           => l_user_id                                                               
                                                            ,p_x_mimes_err_code  => l_x_mimes_error_code
                                                            ,p_x_mimes_err_msg   => l_x_mimes_error_msg);
    END IF;

    IF l_loaded_ebs_trx_status_tbl.COUNT > 0 THEN
        MROI_TT_STATUS_UTIL_PKG.update_trx_status_batch(p_trx_status_tbl     => l_loaded_ebs_trx_status_tbl  
                                                            ,p_user_id           => l_user_id                                                               
                                                            ,p_x_mimes_err_code  => l_x_mimes_error_code
                                                            ,p_x_mimes_err_msg   => l_x_mimes_error_msg);
    END IF;

    IF l_e_trx_status_tbl.COUNT > 0 THEN
        MROI_TT_STATUS_UTIL_PKG.update_trx_status_batch(p_trx_status_tbl     => l_e_trx_status_tbl  
                                                            ,p_user_id           => l_user_id                                                               
                                                            ,p_x_mimes_err_code  => l_x_mimes_error_code
                                                            ,p_x_mimes_err_msg   => l_x_mimes_error_msg);
    END IF;


END process_records;

PROCEDURE CREATE_HR_LOCATIONS(  P_INSTANCE_ID IN NUMBER,
                                P_MIN_INDEX IN NUMBER,
                                P_MAX_INDEX IN NUMBER)
IS

    l_dbg_msg VARCHAR2(4000);
    l_user_id NUMBER := MROI_UTIL_PKG.get_user_id('CONVERSION'); 
	l_x_mroi_error_code NUMBER;
	l_x_mroi_error_msg	VARCHAR2(2000);
	l_x_mimes_error_code NUMBER;
	l_x_mimes_error_msg VARCHAR2(2000);
	l_x_error_count		NUMBER;
    e_proc_exception    EXCEPTION;
    e_mimes_exception   EXCEPTION;
BEGIN 

        validate_records(   P_INSTANCE_ID           => P_INSTANCE_ID, 
                            P_MIN_INDEX             => P_MIN_INDEX, 
                            P_MAX_INDEX             => P_MAX_INDEX,                    
                            p_x_error_count 		=> l_x_error_count,
                            p_x_mroi_error_code 	=> l_x_mroi_error_code,
                            p_x_mroi_error_msg 		=> l_x_mroi_error_msg,
                            p_x_mimes_error_code 	=> l_x_mimes_error_code,
                            p_x_mimes_error_msg 	=> l_x_mimes_error_msg
        );
        
        IF l_x_mroi_error_code = 1 THEN
            RAISE e_proc_exception;
        END IF;
        
        IF l_x_mimes_error_code = 1 THEN
            RAISE e_mimes_exception;
        END IF;
        
        write_log('Number of records with validation errors: '||l_x_error_count);
        print_transaction_status (P_INSTANCE_ID);   
        
        
        process_records(    P_INSTANCE_ID           => P_INSTANCE_ID, 
                            P_MIN_INDEX             => P_MIN_INDEX, 
                            P_MAX_INDEX             => P_MAX_INDEX,                    
                            p_x_error_count 		=> l_x_error_count,
                            p_x_mroi_error_code 	=> l_x_mroi_error_code,
                            p_x_mroi_error_msg 		=> l_x_mroi_error_msg,
                            p_x_mimes_error_code 	=> l_x_mimes_error_code,
                            p_x_mimes_error_msg 	=> l_x_mimes_error_msg
        );
        IF l_x_mroi_error_code = 1 THEN
            RAISE e_proc_exception;
        END IF;
        
        IF l_x_mimes_error_code = 1 THEN
            RAISE e_mimes_exception;
        END IF;
        
        --write_log('Number of records with API errors: '||l_x_error_count);
        --print_transaction_status (P_INSTANCE_ID);   


    
END CREATE_HR_LOCATIONS;

PROCEDURE PROCESS_HR_LOCATION(
                                p_x_errbuf OUT VARCHAR2,
                                p_x_retcode OUT NUMBER,
                                P_MODE IN VARCHAR2,
                                P_FILE_NAME IN VARCHAR2,
                                P_INSTANCE_ID IN NUMBER,
                                P_MIN_INDEX IN NUMBER,
                                P_MAX_INDEX IN NUMBER
                            )
IS 

l_dbg_msg VARCHAR2(4000);
lv_record_count NUMBER;
lv_parallel_count NUMBER := 1;
lv_count_per_instance NUMBER;
lv_start_date DATE := SYSDATE;
lv_instance_id NUMBER;
lv_concurrent_request_id NUMBER := FND_GLOBAL.CONC_REQUEST_ID;
lv_concurrent_program_name VARCHAR2(4000);
l_user_id NUMBER := MROI_UTIL_PKG.get_user_id('CONVERSION'); 
l_tech_id NUMBER;
l_tech_type VARCHAR2(150);
l_process_id  NUMBER;
l_file_name VARCHAR2(1000) := '';
e_mimes_exception EXCEPTION;  
e_proc_exception EXCEPTION;      
e_missing_params EXCEPTION;
l_curr_date_time	VARCHAR2(20) := TO_CHAR(sysdate, 'MMDDYYYYHH24MISS');
l_archive_filename	VARCHAR2(1000);
l_bad_filename	    VARCHAR2(1000);
l_sql_str VARCHAR2(4000);

l_x_mroi_error_code NUMBER;
l_x_mroi_error_msg	VARCHAR2(2000);
l_x_mimes_error_code NUMBER;
l_x_mimes_error_msg VARCHAR2(2000);


BEGIN
    gv_proc_name := 'PROCESS_HR_LOCATION';
    l_dbg_msg := '001. HR Location Started';
    
    write_log('HR Location Conversion Started at ' || TO_CHAR(lv_start_date, 'DD-MON-YYYYY HH24:MI:SS'));
    write_log('--------------------------------------------------------------------------');
    write_log('Parameter');
    write_log('--------------------------------------------------------------------------');
    write_log('P_MODE: ' || P_MODE);
    write_log('P_FILE_NAME: ' || P_FILE_NAME);
    write_log('');

	l_dbg_msg := '100.00 Check if parameters are populated.';
	
	-- Check if the Mode, Filename, Lookup Type, and Application Name  parameters are provided
	IF p_mode IS NULL OR p_file_name IS NULL  THEN
		write_log('Mode or Filename parameters are not populated.');
		RAISE e_missing_params;
	END IF;
    
    
    --parent process
    IF P_INSTANCE_ID IS NULL THEN
    
    
        l_dbg_msg := '100.01 Altering external table MROI_HR_LOCATION_HDR_STG_EXT.';
        --write_log('Altering external table MROI_HR_LOCATION_HDR_STG_EXT with FILENAME as: '||P_FILE_NAME);
        --write_log('Altering external table MROI_HR_LOCATION_HDR_STG_EXT with BADFILE as: MROI_HR_LOCATION_EXT_'||l_curr_date_time||'.bad');
        --write_log('Altering external table MROI_HR_LOCATION_HDR_STG_EXT with LOGFILE as: MROI_HR_LOCATION_EXT_'||l_curr_date_time||'.log');
        l_sql_str := 'ALTER TABLE MROI_HR_LOCATION_HDR_STG_EXT ACCESS PARAMETERS (RECORDS DELIMITED BY NEWLINE
                    BADFILE MROI_HR_LOC_CNV_ARCHIVE:''MROI_HR_LOCATION_EXT_'||l_curr_date_time||'.bad'''||chr(10)||
                    'LOGFILE MROI_HR_LOC_CNV_ARCHIVE:''MROI_HR_LOCATION_EXT_'||l_curr_date_time||'.log'''||chr(10)||
                    'SKIP 1
                    FIELDS CSV WITHOUT EMBEDDED
                    lrtrim
                    MISSING FIELD VALUES ARE NULL
                    ) LOCATION ('''||P_FILE_NAME||''')';
                    
        EXECUTE IMMEDIATE (l_sql_str);
        --EXECUTE IMMEDIATE('TRUNCATE TABLE XXMRO.MROI_HR_LOCATION_ERR');
                                              
        --lv_instance_id := 1;        
        l_tech_id := lv_concurrent_request_id;
        l_tech_type := 'CONC_REQ';
        l_process_id := MROI_TT_STATUS_UTIL_PKG.get_process_id('MROI_HR_LOC_CNV');--(p_process_name)
        l_file_name := P_FILE_NAME;
        
        l_dbg_msg := '100.02 Calling MROI_TT_STATUS_UTIL_PKG.initialize_instance. ';
        MROI_TT_STATUS_UTIL_PKG.initialize_instance(p_tech_id        => l_tech_id
                                                   ,p_tech_type      => l_tech_type -- possible values are SOA_COMPOSITE, SOA_BPEL, CONC_REQ, ODI_LOAD_PLAN
                                                   ,p_process_id     => l_process_id
                                                   ,P_FILE_NAME      => l_file_name
                                                   ,p_start_date     => SYSDATE                                                                                             
                                                   ,p_ec_flag        => 'N' -- values Y or N --indicates if instance has been invoked by reprocessing module (Y will be used only when reprocessing functionality implemented)
                                                   ,p_user_id        => l_user_id
                                                   ,p_attribute1     => NULL
                                                   ,p_attribute2     => NULL
                                                   ,p_attribute3     => NULL
                                                   ,p_attribute4     => NULL
                                                   ,p_attribute5     => NULL
                                                   ,p_x_instance_id  => lv_instance_id
                                                   ,p_x_mimes_err_code => l_x_mimes_error_code
                                                   ,p_x_mimes_err_msg  => l_x_mimes_error_msg);
                                                   
                                                   
        write_log('MIMES Instance ID: ' || lv_instance_id || ' (status = ' || get_Instance_status(lv_instance_id) || ')');                                                   
                                                                                                
         IF l_x_mimes_error_code = 1 THEN                                            
            RAISE e_mimes_exception;                                             
         END IF;  

        l_dbg_msg := '100.03 Calling stage_file_records procedure.';
        --write_log('Calling stage_file_records procedure.');        
        stage_file_records (
				p_instance_id => lv_instance_id,
				p_user_id => l_user_id,
				p_x_total_txn_count => lv_record_count,
				p_x_mroi_error_code 	=> l_x_mroi_error_code,
				p_x_mroi_error_msg 		=> l_x_mroi_error_msg,
				p_x_mimes_error_code 	=> l_x_mimes_error_code,
				p_x_mimes_error_msg 	=> l_x_mimes_error_msg
				);        
        --print_transaction_status (lv_instance_id);    
        IF l_x_mroi_error_code = 1 THEN
            RAISE e_proc_exception;
        END IF;
        
        IF l_x_mimes_error_code = 1 THEN
            RAISE e_mimes_exception;
        END IF;

        l_dbg_msg := '100.04 Calling MROI_UTIL_PKG.archive_file to archive conversion file.';
        --write_log('Calling MROI_UTIL_PKG.archive_file to archive conversion file.');    
        /*
        MROI_UTIL_PKG.archive_file(p_file_name  => p_file_name
                            ,p_file_directory  => 'MROI_HR_LOC_CNV'
                            ,p_file_archive_directory => 'MROI_HR_LOC_CNV_ARCHIVE'                       
                            ,p_archive_file_name_list => l_archive_filename
                            ,p_error_msg => l_x_mroi_error_msg);
                            
        write_log('Archive file: '||l_archive_filename);
                            
        IF l_x_mroi_error_msg IS NOT NULL THEN
            RAISE e_mimes_exception;
        END IF;
        */
    
        l_dbg_msg := '100.06 Calling MROI_TT_STATUS_UTIL_PKG.update_instance_status to set status to: '||MROI_TT_STATUS_UTIL_PKG.G_INSTCODE_RECS_STAGED;
        --write_log('Calling MROI_TT_STATUS_UTIL_PKG.update_instance_status to set status to: '||MROI_TT_STATUS_UTIL_PKG.G_INSTCODE_RECS_STAGED);
        MROI_TT_STATUS_UTIL_PKG.update_instance_status(
                                     p_instance_id         => lv_instance_id
                                    ,p_status_code         => MROI_TT_STATUS_UTIL_PKG.G_INSTCODE_RECS_STAGED
                                    ,p_archive_file_name   => l_archive_filename
                                    ,p_received_file_count => 1
                                    ,p_header_trx_count    => NULL
                                    ,p_received_trx_count  => lv_record_count
                                    ,p_header_dollar_amt   => NULL                                
                                    ,p_received_dollar_amt => NULL                               
                                    ,p_sent_trx_count      => NULL
                                    ,p_files_sequence      => NULL
                                    ,p_user_id             => l_user_id
                                    ,p_attribute1          => NULL
                                    ,p_attribute2          => NULL
                                    ,p_attribute3          => NULL
                                    ,p_attribute4          => NULL
                                    ,p_attribute5          => NULL
                                    ,p_x_mimes_err_code    => l_x_mimes_error_code
                                    ,p_x_mimes_err_msg     => l_x_mimes_error_msg);
                                    
        write_log('MIMES Instance ID: ' || lv_instance_id || ' (status = ' || get_Instance_status(lv_instance_id) || ')');                                                   
                                    
        IF l_x_mimes_error_code = 1 THEN                                            
            RAISE e_mimes_exception;                                             
        END IF;    
                
    
        l_bad_filename := 'MROI_HR_LOCATION_EXT_'||l_curr_date_time||'.bad';
        l_dbg_msg := '100.05 Calling MROI_UTIL_PKG.file_exists to check if a BAD file was generated.';
        --write_log('Calling MROI_UTIL_PKG.file_exists to check if a BAD file was generated.');
        IF MROI_UTIL_PKG.file_exists(l_bad_filename, 'MROI_HR_LOC_CNV_ARCHIVE') THEN
            write_log('WARNING----> BAD File: '||l_bad_filename||' has been generated for this conversion run, please review the file and make any necessary corrections.');
        END IF;
        

               
       --check parallel processing if applicable        
       IF lv_record_count <= 100 THEN
            lv_parallel_count := 1;
       ELSE
            lv_parallel_count := get_parallel_count(lv_concurrent_request_id);
            lv_count_per_instance := CEIL(lv_record_count / lv_parallel_count );
            write_log('Parallel Count: ' || TO_CHAR(lv_parallel_count));
            write_log('Count Per Instance: ' || TO_CHAR(lv_count_per_instance));                    
        END IF;    
        
        IF lv_parallel_count = 1 THEN
            create_hr_locations(lv_instance_id, NULL,NULL);        
    
            l_dbg_msg := '100.08 Calling MROI_TT_STATUS_UTIL_PKG.update_instance_status to set status to MROI_TT_STATUS_UTIL_PKG.G_INSTCODE_RECS_VALIDATED.';
            --write_log('Calling MROI_TT_STATUS_UTIL_PKG.update_instance_status to set status to MROI_TT_STATUS_UTIL_PKG.G_INSTCODE_RECS_VALIDATED.');
            MROI_TT_STATUS_UTIL_PKG.update_instance_status(
                                         p_instance_id         => lv_instance_id
                                        ,p_status_code         => MROI_TT_STATUS_UTIL_PKG.G_INSTCODE_RECS_VALIDATED
                                        ,p_archive_file_name   => NULL
                                        ,p_received_file_count => NULL
                                        ,p_header_trx_count    => NULL
                                        ,p_received_trx_count  => NULL
                                        ,p_header_dollar_amt   => NULL                                
                                        ,p_received_dollar_amt => NULL                               
                                        ,p_sent_trx_count      => NULL
                                        ,p_files_sequence      => NULL
                                        ,p_user_id             => l_user_id
                                        ,p_attribute1          => NULL
                                        ,p_attribute2          => NULL
                                        ,p_attribute3          => NULL
                                        ,p_attribute4          => NULL
                                        ,p_attribute5          => NULL
                                        ,p_x_mimes_err_code    => l_x_mimes_error_code
                                        ,p_x_mimes_err_msg     => l_x_mimes_error_msg);

            write_log('MIMES Instance ID: ' || lv_instance_id || ' (status = ' || get_Instance_status(lv_instance_id) || ')');                                                   
                                        
            IF l_x_mimes_error_code = 1 THEN                                            
                RAISE e_mimes_exception;                                             
            END IF;     
            
        ELSE        
            spawn_parallel_process (P_MODE, lv_concurrent_request_id, lv_instance_id, lv_record_count, lv_parallel_count );        
    
            l_dbg_msg := '100.08 Calling MROI_TT_STATUS_UTIL_PKG.update_instance_status to set status to MROI_TT_STATUS_UTIL_PKG.G_INSTCODE_RECS_VALIDATED.';
            --write_log('Calling MROI_TT_STATUS_UTIL_PKG.update_instance_status to set status to MROI_TT_STATUS_UTIL_PKG.G_INSTCODE_RECS_VALIDATED.');
            MROI_TT_STATUS_UTIL_PKG.update_instance_status(
                                         p_instance_id         => lv_instance_id
                                        ,p_status_code         => MROI_TT_STATUS_UTIL_PKG.G_INSTCODE_RECS_VALIDATED
                                        ,p_archive_file_name   => NULL
                                        ,p_received_file_count => NULL
                                        ,p_header_trx_count    => NULL
                                        ,p_received_trx_count  => NULL
                                        ,p_header_dollar_amt   => NULL                                
                                        ,p_received_dollar_amt => NULL                               
                                        ,p_sent_trx_count      => NULL
                                        ,p_files_sequence      => NULL
                                        ,p_user_id             => l_user_id
                                        ,p_attribute1          => NULL
                                        ,p_attribute2          => NULL
                                        ,p_attribute3          => NULL
                                        ,p_attribute4          => NULL
                                        ,p_attribute5          => NULL
                                        ,p_x_mimes_err_code    => l_x_mimes_error_code
                                        ,p_x_mimes_err_msg     => l_x_mimes_error_msg);

            write_log('MIMES Instance ID: ' || lv_instance_id || ' (status = ' || get_Instance_status(lv_instance_id) || ')');                                                   
                                        
            IF l_x_mimes_error_code = 1 THEN                                            
                RAISE e_mimes_exception;                                             
            END IF;     
            
        END IF;

    ELSE --child process
    
       lv_instance_id := P_INSTANCE_ID;
       create_hr_locations(lv_instance_id, P_MIN_INDEX, P_MAX_INDEX);
    END IF; 
    
    
    l_dbg_msg := '100.08 Calling MROI_TT_STATUS_UTIL_PKG.update_instance_status to set status to MROI_TT_STATUS_UTIL_PKG.G_INSTCODE_RECS_LOADED_OIT_API.';
    MROI_TT_STATUS_UTIL_PKG.update_instance_status(
                                         p_instance_id         => lv_instance_id
                                        ,p_status_code         => MROI_TT_STATUS_UTIL_PKG.G_INSTCODE_RECS_LOADED_OIT_API
                                        ,p_archive_file_name   => NULL
                                        ,p_received_file_count => NULL
                                        ,p_header_trx_count    => NULL
                                        ,p_received_trx_count  => NULL
                                        ,p_header_dollar_amt   => NULL                                
                                        ,p_received_dollar_amt => NULL                               
                                        ,p_sent_trx_count      => NULL
                                        ,p_files_sequence      => NULL
                                        ,p_user_id             => l_user_id
                                        ,p_attribute1          => NULL
                                        ,p_attribute2          => NULL
                                        ,p_attribute3          => NULL
                                        ,p_attribute4          => NULL
                                        ,p_attribute5          => NULL
                                        ,p_x_mimes_err_code    => l_x_mimes_error_code
                                        ,p_x_mimes_err_msg     => l_x_mimes_error_msg);

    write_log('MIMES Instance ID: ' || lv_instance_id || ' (status = ' || get_Instance_status(lv_instance_id) || ')');                                                   
                                        
    IF l_x_mimes_error_code = 1 THEN                                            
                RAISE e_mimes_exception;                                             
    END IF;     
    
    l_dbg_msg := '100.08 Calling MROI_TT_STATUS_UTIL_PKG.update_instance_status to set status to MROI_TT_STATUS_UTIL_PKG.G_INSTCODE_COMPLETED.';
    MROI_TT_STATUS_UTIL_PKG.update_instance_status(
                                         p_instance_id         => lv_instance_id
                                        ,p_status_code         => MROI_TT_STATUS_UTIL_PKG.G_INSTCODE_COMPLETED
                                        ,p_archive_file_name   => NULL
                                        ,p_received_file_count => NULL
                                        ,p_header_trx_count    => NULL
                                        ,p_received_trx_count  => NULL
                                        ,p_header_dollar_amt   => NULL                                
                                        ,p_received_dollar_amt => NULL                               
                                        ,p_sent_trx_count      => NULL
                                        ,p_files_sequence      => NULL
                                        ,p_user_id             => l_user_id
                                        ,p_attribute1          => NULL
                                        ,p_attribute2          => NULL
                                        ,p_attribute3          => NULL
                                        ,p_attribute4          => NULL
                                        ,p_attribute5          => NULL
                                        ,p_x_mimes_err_code    => l_x_mimes_error_code
                                        ,p_x_mimes_err_msg     => l_x_mimes_error_msg);

    write_log('MIMES Instance ID: ' || lv_instance_id || ' (status = ' || get_Instance_status(lv_instance_id) || ')');                                                   
                                        
    IF l_x_mimes_error_code = 1 THEN                                            
                RAISE e_mimes_exception;                                             
    END IF;     
    
 
    generate_report (lv_instance_id);
    write_outfile(lv_instance_id);
 
    write_log('*************************************************************************');
	write_log('Conversion Finished '||TO_CHAR(sysdate, 'DD-MON-RRRR HH24:MI:SS'));
	write_log('*************************************************************************');	
    
    p_x_errbuf := '';
    p_x_retcode := 0;
EXCEPTION
	WHEN e_mimes_exception THEN
		p_x_errbuf := 'MIMES error for Instance ID: '||lv_instance_id||' when - '||l_dbg_msg||' - '||l_x_mimes_error_msg;
		p_x_retcode := 1;
    WHEN e_proc_exception THEN
        p_x_errbuf := 'Error occurred for Instance ID: '||lv_instance_id||' when - '||l_dbg_msg||' - '||l_x_mroi_error_msg;
        p_x_retcode := 1;
	WHEN e_missing_params THEN
		p_x_errbuf := 'Mode or Filename parameters are not populated.';
		p_x_retcode := 2;
	WHEN OTHERS THEN

    
        l_dbg_msg := '100.08 Calling MROI_TT_STATUS_UTIL_PKG.update_instance_status to set status to MROI_TT_STATUS_UTIL_PKG.G_INSTCODE_ERROR.';
        MROI_TT_STATUS_UTIL_PKG.update_instance_status(
                                             p_instance_id         => lv_instance_id
                                            ,p_status_code         => MROI_TT_STATUS_UTIL_PKG.G_INSTCODE_ERROR
                                            ,p_archive_file_name   => NULL
                                            ,p_received_file_count => NULL
                                            ,p_header_trx_count    => NULL
                                            ,p_received_trx_count  => NULL
                                            ,p_header_dollar_amt   => NULL                                
                                            ,p_received_dollar_amt => NULL                               
                                            ,p_sent_trx_count      => NULL
                                            ,p_files_sequence      => NULL
                                            ,p_user_id             => l_user_id
                                            ,p_attribute1          => NULL
                                            ,p_attribute2          => NULL
                                            ,p_attribute3          => NULL
                                            ,p_attribute4          => NULL
                                            ,p_attribute5          => NULL
                                            ,p_x_mimes_err_code    => l_x_mimes_error_code
                                            ,p_x_mimes_err_msg     => l_x_mimes_error_msg);
    
        write_log('MIMES Instance ID: ' || lv_instance_id || ' (status = ' || get_Instance_status(lv_instance_id) || ')');                                                   
         
		p_x_errbuf := 'Other exception occurred when '||l_dbg_msg||' - '||SQLERRM;
		p_x_retcode := 2;            
END PROCESS_HR_LOCATION;
                         
                         
                                                           
END MROI_HR_LOCATION_CONV_PKG;
