create or replace PACKAGE BODY MROI_AHL_ITEM_GROUP_CONV_PKG AS
 /* ***************************************************************************
 /*  Name:          MROI_AHL_ITEM_GROUP_CONV_PKG 
 /*  Object Type:   Package Body
 /*  Description:   Package Body for Item Group Conversion
 /*
 /*  RICE Type:     Conversion
 /*  RICE ID:    	0007A
 /*
 /*  Change History:
 /*  Date          Name                           Ver      Modification
 /*  -----------   ----------------------         ----     -------------
 /*  06/05/2019    Sarah Gadzinski                1.0      Initial Creation
 /******************************************************************************/
 
 /* ***************************************************************************
 /*  Name:          main 
 /*  Object Type:   Procedure
 /*  Description:   This is the main conversion procedure registered as a 
 /*                 concurrent program
 /*
 /*  Calls:         stage_file_records, load_records, write_log,
 /*                 MROI_TT_STATUS_UTIL_PKG, MROI_UTIL_PKG
 /*  Source:        MROI_AHL_ITEM_GROUP_CONV_PKG.pkb
 /*  Parameters:    p_x_errbuf - OUT parameter for concurrent program
 /*					p_x_retcode - OUT parameter for concurrent program
 /*					p_mode - IN parameter for Validate (V) or Load (L) mode
 /*					p_filename - IN parameter for conversion filename
 /*  Return values: N/A
 /*
 /*  Change History:
 /*  Date          Name                           Ver      Modification
 /*  -----------   ----------------------         ----     -------------
 /*  06/05/2019    Sarah Gadzinski                 1.0      Initial Creation
 /******************************************************************************/
PROCEDURE main (
				p_x_errbuf OUT VARCHAR2,
				p_x_retcode OUT NUMBER,
				p_mode IN VARCHAR2,
				p_filename IN VARCHAR2
				)
IS
	l_dbg_msg			VARCHAR2(2000);
	l_instance_id		NUMBER := -1;
	l_user_id			NUMBER;
	l_conc_req_id		NUMBER;
	l_curr_date_time	VARCHAR2(20) := TO_CHAR(SYSDATE, 'MMDDYYYYHH24MISS');
	l_sql_str			VARCHAR2(1000);
	l_archive_filename	VARCHAR2(1000);
    l_bad_filename	    VARCHAR2(1000);
	
	l_x_total_txn_count NUMBER;
	l_x_mroi_error_code NUMBER;
	l_x_mroi_error_msg	VARCHAR2(2000);
	l_x_mimes_error_code NUMBER;
	l_x_mimes_error_msg VARCHAR2(2000);
	l_x_error_count		NUMBER;
	
	e_mimes_exception	EXCEPTION;
	e_proc_exception	EXCEPTION;
	e_missing_params	EXCEPTION;
    e_bad_file			EXCEPTION;
    
BEGIN
	write_log('*************************************************************************');
	write_log('Conversion started '||TO_CHAR(SYSDATE, 'DD-MON-RRRR HH24:MI:SS'));
	write_log('Parameters Entered:');
	write_log('p_mode: '||p_mode);
	write_log('p_filename: '||p_filename);
	write_log('*************************************************************************');
	
	l_dbg_msg := '100.00 Check if parameters are populated.';
	
	-- Check if the Mode and Filename parameters are provided
	IF (p_mode IS NULL OR p_filename IS NULL) THEN
		write_log('Mode or Filename parameters are not populated.');
		RAISE e_missing_params;
	END IF;
    
    /*l_dbg_msg := '100.00.01 Calling fnd_global.apps_initialize for CONVERSION user.';
    write_log('Calling apps_initialize for CONVERSION user.');
    fnd_global.apps_initialize(
	user_id => 1160,
	resp_id => 50110,
	resp_appl_id => 867) ;

    l_dbg_msg := '100.00.02 Calling mo_global.init for AHL.';
    write_log('Calling mo_global.init for AHL.');
    mo_global.init('AHL'); */

	l_user_id := FND_GLOBAL.user_id;
	
	l_dbg_msg := '100.01 Altering external table MROI_AHL_ITEM_GROUP_EXT.';
	write_log('Altering external table MROI_AHL_ITEM_GROUP_EXT with FILENAME as: '||p_filename);
	write_log('Altering external table MROI_AHL_ITEM_GROUP_EXT with BADFILE as: MROI_AHL_ITEM_GROUP_EXT_'||l_curr_date_time||'.bad');
	write_log('Altering external table MROI_AHL_ITEM_GROUP_EXT with LOGFILE as: MROI_AHL_ITEM_GROUP_EXT_'||l_curr_date_time||'.log');
	l_sql_str := 'ALTER TABLE XXMRO.MROI_AHL_ITEM_GROUP_EXT ACCESS PARAMETERS (RECORDS DELIMITED BY NEWLINE
				BADFILE MROI_ITEM_GRP_CNV_ARCHIVE:''MROI_AHL_ITEM_GROUP_EXT_'||l_curr_date_time||'.bad'''||chr(10)||
				'LOGFILE MROI_ITEM_GRP_CNV_ARCHIVE:''MROI_AHL_ITEM_GROUP_EXT_'||l_curr_date_time||'.log'''||chr(10)||
				'SKIP 1
				 FIELDS TERMINATED BY '','' OPTIONALLY ENCLOSED BY ''"''
                 lrtrim
				MISSING FIELD VALUES ARE NULL
				) LOCATION ('''||p_filename||''')';
				
	EXECUTE IMMEDIATE (l_sql_str);
    
    l_dbg_msg := '100.01.1 Selecting from external table MROI_AHL_ITEM_GROUP_EXT.';
	write_log('Selecting from external table MROI_AHL_ITEM_GROUP_EXT.');
    
	SELECT COUNT(*)
    INTO l_x_total_txn_count
    FROM MROI_AHL_ITEM_GROUP_EXT;
	
    l_bad_filename := 'MROI_AHL_ITEM_GROUP_EXT_'||l_curr_date_time||'.bad';
    l_dbg_msg := '100.01.2 Calling MROI_UTIL_PKG.file_exists to check if a BAD file was generated.';
	write_log('Calling MROI_UTIL_PKG.file_exists to check if a BAD file was generated.');
    IF MROI_UTIL_PKG.file_exists(l_bad_filename, 'MROI_ITEM_GRP_CNV_ARCHIVE') THEN
        write_log('ERROR----> BAD File: '||l_bad_filename||' has been generated for this conversion run, please review the file and make any necessary corrections.');
		RAISE e_bad_file;
    END IF;
	
	l_dbg_msg := '100.02 Calling MROI_TT_STATUS_UTIL_PKG.initialize_instance.';
	write_log('Calling MROI_TT_STATUS_UTIL_PKG.initialize_instance.');
    write_log('fnd_global.conc_request_id '||fnd_global.conc_request_id);
	MROI_TT_STATUS_UTIL_PKG.initialize_instance(
							  p_tech_id          => fnd_global.conc_request_id
                             ,p_tech_type        => 'CONC_REQ'
                             ,p_process_id       => MROI_TT_STATUS_UTIL_PKG.get_process_id('MROI_ITEM_GRP_CNV')
                             ,p_file_name        => p_filename
                             ,p_start_date       => SYSDATE                             
                             ,p_ec_flag          => 'N'
                             ,p_user_id          => l_user_id
                             ,p_attribute1       => NULL
                             ,p_attribute2       => NULL
                             ,p_attribute3       => NULL
                             ,p_attribute4       => NULL
                             ,p_attribute5       => NULL
                             ,p_x_instance_id    => l_instance_id
                             ,p_x_mimes_err_code => l_x_mimes_error_code
                             ,p_x_mimes_err_msg  => l_x_mimes_error_msg);
							 
	IF l_x_mimes_error_code = 1 THEN
		RAISE e_mimes_exception;
	END IF;
	
	l_dbg_msg := '100.03 Calling stage_file_records procedure.';
	write_log('Calling stage_file_records procedure.');
	stage_file_records (
				p_instance_id 			=> l_instance_id,
				p_user_id 				=> l_user_id,
				p_x_total_txn_count		=> l_x_total_txn_count,
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
	
	l_dbg_msg := '100.04 Calling MROI_UTIL_PKG.archive_file to archive conversion file.';
	write_log('Calling MROI_UTIL_PKG.archive_file to archive conversion file.');

	MROI_UTIL_PKG.archive_file(p_file_name  => p_filename
                              ,p_file_directory  => 'MROI_ITEM_GRP_CNV'
                              ,p_file_archive_directory => 'MROI_ITEM_GRP_CNV_ARCHIVE'                       
                              ,p_archive_file_name_list => l_archive_filename
                              ,p_error_msg => l_x_mroi_error_msg);
                        
    write_log('Archive file: '||l_archive_filename);
                        
    IF l_x_mroi_error_msg IS NOT NULL THEN
        RAISE e_proc_exception;
	END IF;
    
  /* l_bad_filename := 'MROI_AHL_ITEM_GROUP_EXT_'||l_curr_date_time||'.bad';
    l_dbg_msg := '100.05 Calling MROI_UTIL_PKG.file_exists to check if a BAD file was generated.';
	write_log('Calling MROI_UTIL_PKG.file_exists to check if a BAD file was generated.');
    IF MROI_UTIL_PKG.file_exists(l_bad_filename, 'MROI_ITEM_GRP_CNV_ARCHIVE') THEN
        write_log('WARNING----> BAD File: '||l_bad_filename||' has been generated for this conversion run, please review the file and make any necessary corrections.');
    END IF;*/
	
	l_dbg_msg := '100.06 Calling MROI_TT_STATUS_UTIL_PKG.update_instance_status to set status to: '||MROI_TT_STATUS_UTIL_PKG.G_INSTCODE_RECS_STAGED;
	write_log('Calling MROI_TT_STATUS_UTIL_PKG.update_instance_status to set status to: '||MROI_TT_STATUS_UTIL_PKG.G_INSTCODE_RECS_STAGED);
	
	MROI_TT_STATUS_UTIL_PKG.update_instance_status(
								 p_instance_id         => l_instance_id
								,p_status_code         => MROI_TT_STATUS_UTIL_PKG.G_INSTCODE_RECS_STAGED
								,p_archive_file_name   => l_archive_filename
								,p_received_file_count => 1
								,p_header_trx_count    => NULL
								,p_received_trx_count  => l_x_total_txn_count
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
                                
    IF l_x_mimes_error_code = 1 THEN                                            
        RAISE e_mimes_exception;                                             
    END IF;    
    
    -- Perform validations only when Execute Mode is L or V
    IF (p_mode = 'L' OR p_mode = 'V') THEN
    
        l_dbg_msg := '100.07 Calling validate_records procedure.';
        write_log('Calling validate_records procedure.');
        validate_records (
                    p_instance_id 			=> l_instance_id,
                    p_user_id 				=> l_user_id,
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
        
        l_dbg_msg := '100.08 Calling MROI_TT_STATUS_UTIL_PKG.update_instance_status to set status to '||MROI_TT_STATUS_UTIL_PKG.G_INSTCODE_RECS_VALIDATED;
        write_log('Calling MROI_TT_STATUS_UTIL_PKG.update_instance_status to set status to '||MROI_TT_STATUS_UTIL_PKG.G_INSTCODE_RECS_VALIDATED);
        MROI_TT_STATUS_UTIL_PKG.update_instance_status(
                                     p_instance_id         => l_instance_id
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
                                    
        IF l_x_mimes_error_code = 1 THEN                                            
            RAISE e_mimes_exception;                                             
        END IF; 
        
    END IF; -- IF p_mode = 'L' OR p_mode = 'V'
   
   
	write_log('*************************************************************************');
    write_log('Number of records staged: '||l_x_total_txn_count);
    write_log('Number of validation error records: '||l_x_error_count);
   -- write_log('Number of API error records: '||l_x_api_error_count);
	write_log('Conversion Finished '||TO_CHAR(sysdate, 'DD-MON-RRRR HH24:MI:SS'));
	write_log('*************************************************************************');	

    p_x_errbuf := '';
    p_x_retcode := 0;
EXCEPTION
	WHEN e_mimes_exception THEN
		p_x_errbuf := 'MIMES error for Instance ID: '||l_instance_id||' when - '||l_dbg_msg||' - '||l_x_mimes_error_msg;
		p_x_retcode := 1;
    WHEN e_proc_exception THEN
        p_x_errbuf := 'Error occurred for Instance ID: '||l_instance_id||' when - '||l_dbg_msg||' - '||l_x_mroi_error_msg;
        p_x_retcode := 1;
	WHEN e_missing_params THEN
		p_x_errbuf := 'Mode or Filename parameters are not populated.';
		p_x_retcode := 2;
    WHEN e_bad_file THEN
		p_x_errbuf := 'Bad file has been generated for this conversion run.';
		p_x_retcode := 2;
	WHEN OTHERS THEN
		p_x_errbuf := 'Other exception occurred when '||l_dbg_msg||' - '||SQLERRM;
		p_x_retcode := 2;
END main;

 /* ***************************************************************************
 /*  Name:          stage_file_records 
 /*  Object Type:   Procedure
 /*  Description:   Procedure to read from external table and load into staging 
 /*                 table, then invoke MIMES
 /*
 /*  Calls:         MROI_TT_STATUS_UTIL_PKG
 /*  Source:        MROI_AHL_ITEM_GROUP_CONV_PKG.pkb
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
 /*  06/05/2019    Sarah Gadzinski                 1.0      Initial Creation
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
	
	l_dbg_msg := '200.01 Inserting records into MROI_AHL_ITEM_GROUP_HDR_STG table.';
	write_log('Inserting records into MROI_AHL_ITEM_GROUP_HDR_STG table.');

	INSERT INTO MROI_AHL_ITEM_GROUP_HDR_STG (
		RECORD_HEADER_ID,
		INSTANCE_ID,
		GROUP_NAME,
        DESCRIPTION,
        TYPE,
        ITEM,
        ITEM_DESCRIPTION,
        QUANTITY,
        UOM,
        PRIORITY,
        INTERCHANGEABILITY_TYPE,
		CREATION_DATE,
		LAST_UPDATE_DATE,
		CREATED_BY,
		LAST_UPDATED_BY) 
		SELECT	MROI_AHL_ITEM_GROUP_HDR_STG_S.nextval,
				p_instance_id,
				GROUP_NAME,
                DESCRIPTION,
                TYPE,
                ITEM,
                ITEM_DESCRIPTION,
                QUANTITY,
                UOM,
                PRIORITY,
                INTERCHANGEABILITY_TYPE,
				SYSDATE,
				SYSDATE,
				p_user_id,
				p_user_id
		FROM MROI_AHL_ITEM_GROUP_EXT;
	
	l_x_total_txn_count := SQL%ROWCOUNT;
	write_log('Number of records inserted into MROI_AHL_ITEM_GROUP_HDR_STG table: '||l_x_total_txn_count);
	
	p_x_total_txn_count := l_x_total_txn_count;
	
	COMMIT;
	
	l_dbg_msg := '200.02 Calling MROI_TT_STATUS_UTIL_PKG.initialize_trx_batch.';
	write_log('Calling MROI_TT_STATUS_UTIL_PKG.initialize_trx_batch.');
	MROI_TT_STATUS_UTIL_PKG.initialize_trx_batch(
						p_instance_id       => p_instance_id
                       ,p_user_id           => p_user_id
                       ,p_x_mimes_err_code  => l_x_mimes_error_code
                       ,p_x_mimes_err_msg   => l_x_mimes_error_msg);
	
	IF l_x_mimes_error_code = 1 THEN
		RAISE e_mimes_exception;
	END IF;

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

 /* ***************************************************************************
 /*  Name:          validate_records 
 /*  Object Type:   Procedure
 /*  Description:   Procedure to validate records staged for a specific Instance ID 
 /*                 and invoke MIMES for each transaction to update the status
 /*
 /*  Calls:         MROI_TT_ERROR_UTIL_PKG, MROI_TT_STATUS_UTIL_PKG
 /*  Source:        MROI_AHL_ITEM_GROUP_CONV_PKG_PB.pls
 /*  Parameters:    p_instance_id - IN parameter for MIMES instance ID
 /*                 p_user_id - IN parameter for FND User ID
 /*                 p_x_error_count - OUT parameter for number of error records
 /*                 p_x_mroi_error_code - OUT parameter for error code
 /*                 p_x_mroi_error_msg - OUT parameter for error message
 /*                 p_x_mimes_error_code - OUT parameter for MIMES error code
 /*                 p_x_mimes_error_msg - OUT parameter for MIMES error message
 /*  Return values: N/A
 /*
 /*  Change History:
 /*  Date          Name                           Ver      Modification
 /*  -----------   ----------------------         ----     -------------
 /*  06/20/2019    Sarah Gadzinski                 1.0      Initial Creation
 /******************************************************************************/		
PROCEDURE validate_records (
				p_instance_id IN NUMBER,
				p_user_id IN NUMBER,
				p_x_error_count OUT NUMBER,
				p_x_mroi_error_code OUT NUMBER,
				p_x_mroi_error_msg OUT VARCHAR2,
				p_x_mimes_error_code OUT NUMBER,
				p_x_mimes_error_msg OUT VARCHAR2
				)
IS
	l_dbg_msg				VARCHAR2(2000);

	l_t_err_tbl				MROI_TT_ERROR_UTIL_PKG.t_err_tbl := MROI_TT_ERROR_UTIL_PKG.t_err_tbl();
    l_t_trx_status_tbl		MROI_TT_STATUS_UTIL_PKG.t_trx_status_tbl := MROI_TT_STATUS_UTIL_PKG.t_trx_status_tbl();
	l_x_mimes_error_code 	NUMBER := 0;
	l_x_mimes_error_msg 	VARCHAR2(2000);
	e_mimes_exception		EXCEPTION;
	
	l_error_msg				VARCHAR2(2000);
    
	CURSOR c_invalid_values (p_c_instance_id NUMBER) IS
    SELECT MROI_AHL_ITEM_GROUP_ERR_S.nextval AS error_id, rec.transaction_id, rec.record_level_id, rec.record_level_type, rec.error_field, rec.error_code, rec.error_attribute1, rec.error_attribute2, rec.error_attribute3, rec.error_attribute4, 
    '' as ORA_ERR_CODE, '' as ORA_ERR_MSG, '' as ERR_ADDRESSED_FLAG, '' as ERR_OLD_FLAG
        FROM (
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'GROUP_NAME' as error_field, 13000 as error_code,
            '' as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_AHL_ITEM_GROUP_HDR_STG a
            WHERE a.group_name IS NULL
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'TYPE' as error_field, 13000 as error_code,
            '' as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_AHL_ITEM_GROUP_HDR_STG a
            WHERE a.type IS NULL
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'ITEM' as error_field, 13000 as error_code,
            '' as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_AHL_ITEM_GROUP_HDR_STG a
            WHERE a.item IS NULL
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'UOM' as error_field, 13000 as error_code,
            '' as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_AHL_ITEM_GROUP_HDR_STG a
            WHERE a.uom IS NULL
            UNION ALL
             SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'QUANTITY' as error_field, 13000 as error_code,
            '' as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_AHL_ITEM_GROUP_HDR_STG a
            WHERE a.quantity IS NULL
            UNION ALL
             SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'PRIORITY' as error_field, 13000 as error_code,
            '' as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_AHL_ITEM_GROUP_HDR_STG a
            WHERE a.priority IS NULL
            UNION ALL
             SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'INTERCHANGEABILITY_TYPE' as error_field, 13000 as error_code,
            '' as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_AHL_ITEM_GROUP_HDR_STG a
            WHERE a.interchangeability_type IS NULL
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'TYPE' as error_field, 10001 as error_code,
            a.type as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_AHL_ITEM_GROUP_HDR_STG a
            WHERE EXISTS (SELECT 'x' FROM AHL_ITEM_GROUPS_B b WHERE UPPER(b.name) = UPPER(a.group_name))
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'TYPE' as error_field, 10001 as error_code,
            a.type as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_AHL_ITEM_GROUP_HDR_STG a
            WHERE NOT EXISTS (SELECT 'x' FROM FND_LOOKUPS b WHERE b.meaning = nvl(a.type, 'Tracked') AND b.lookup_type = 'AHL_ITEMGROUP_TYPE' AND b.enabled_flag = 'Y')
            /*UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'ITEM' as error_field, 10001 as error_code,
            a.item as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_AHL_ITEM_GROUP_HDR_STG a
            WHERE NOT EXISTS (SELECT 'x' FROM MTL_SYSTEM_ITEMS_B b WHERE UPPER(segment1 || '.'|| segment2) = UPPER(a.item))*/
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'ITEM' as error_field, 10001 as error_code,
            a.item as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_AHL_ITEM_GROUP_HDR_STG a
            WHERE NOT EXISTS (SELECT 'x' FROM MTL_SYSTEM_ITEMS_B b WHERE UPPER(segment1 || '.'|| segment2) = UPPER(a.item) 
            AND organization_id = (select organization_id from mtl_parameters where UPPER(organization_code) = 'MST'))
             UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'ITEM' as error_field, 10005 as error_code,
            a.item as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_AHL_ITEM_GROUP_HDR_STG a
            WHERE NOT EXISTS (SELECT 'x' FROM MTL_SYSTEM_ITEMS_B b WHERE UPPER(segment1 || '.'|| segment2) = UPPER(a.item)
            AND NVL(comms_nl_trackable_flag, 'N') = DECODE(UPPER(nvl(a.type, 'TRACKED')), 'TRACKED', 'Y', 'NON TRACKED', 'N')
            AND organization_id = (select organization_id from mtl_parameters where UPPER(organization_code) = 'MST'))
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'PRIORITY' as error_field, 10005 as error_code,
            to_char(a.priority) as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_AHL_ITEM_GROUP_HDR_STG a
            WHERE EXISTS (select group_name, priority, count(*) as count from MROI_AHL_ITEM_GROUP_HDR_STG 
                          where instance_Id = a.instance_id
                           and priority = a.priority
                          group by group_name, priority having count(*) > 1)
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'INTERCHANGEABILITY_TYPE' as error_field, 10005 as error_code,
            a.interchangeability_type as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_AHL_ITEM_GROUP_HDR_STG a
            WHERE NOT EXISTS (SELECT 'x' FROM FND_LOOKUPS b WHERE upper(b.meaning) = upper(a.interchangeability_type) AND b.lookup_type = 'AHL_INTERCHANGE_ITEM_TYPE' AND b.enabled_flag = 'Y')
         
            ) rec
        WHERE rec.instance_id = p_c_instance_id;                       
	
BEGIN

	p_x_mroi_error_code := 0;
	p_x_mroi_error_msg := NULL;
	p_x_mimes_error_code := 0;
	p_x_mimes_error_msg := NULL;
	p_x_error_count := 0;
    
    l_dbg_msg := '300.00 Opening cursor FOR LOOP for c_invalid_values using Instance ID: '||p_instance_id;
    write_log('Opening cursor FOR LOOP for c_invalid_values using Instance ID: '||p_instance_id);
    
    OPEN c_invalid_values(p_instance_id);
    FETCH c_invalid_values BULK COLLECT INTO l_t_err_tbl;
    select transaction_id, 
    -1 as instance_id,
    NULL as reprocess_instance_id,
    MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_ERRED_STG as status_code, 
    '' as reject_reason, 
    '' as remarks, '' as oit_reject_reason, 
    '' as oit_remaks, sysdate as creation_date, 
    -1 as created_by,
    sysdate as last_update_date, 
    -1 as last_updated_by
    bulk collect into l_t_trx_status_tbl from table(l_t_err_tbl);
    
    CLOSE c_invalid_values; 
	/*FOR r_invalid_values IN c_invalid_values(p_instance_id) LOOP
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
	END LOOP; -- FOR r_invalid_values IN c_invalid_values(p_instance_id) LOOP*/

	-- Calls MIMES APIs if there are errors found
    IF l_t_err_tbl.COUNT > 0 THEN
   
		-- Call MIMES API to insert collection of erred records into MROI_AHL_ITEM_GROUP_ERR table
		l_dbg_msg := '300.01 Calling MROI_TT_ERROR_UTIL_PKG.log_error_batch for Instance ID: '||p_instance_id;
		write_log('Calling MROI_TT_ERROR_UTIL_PKG.log_error_batch for Instance ID: '||p_instance_id);		
		MROI_TT_ERROR_UTIL_PKG.log_error_batch(p_instance_id       => p_instance_id
											  ,p_user_id           => p_user_id
											  ,p_t_err_tbl         => l_t_err_tbl 
											  ,p_x_mimes_err_code  => l_x_mimes_error_code
											  ,p_x_mimes_err_msg   => l_x_mimes_error_msg);
										  
		IF l_x_mimes_error_code = 1 THEN                                            
			RAISE e_mimes_exception;                                             
		END IF;
		  
		l_dbg_msg := '300.02  Calling MROI_TT_STATUS_UTIL_PKG.update_trx_status_batch for Instance ID: '||p_instance_id;
		write_log('Calling MROI_TT_STATUS_UTIL_PKG.update_trx_status_batch for Instance ID: '||p_instance_id);		
		MROI_TT_STATUS_UTIL_PKG.update_trx_status_batch(p_trx_status_tbl     => l_t_trx_status_tbl  
														,p_user_id           => p_user_id                                                               
														,p_x_mimes_err_code  => l_x_mimes_error_code
														,p_x_mimes_err_msg   => l_x_mimes_error_msg);
		 IF l_x_mimes_error_code = 1 THEN                                            
			RAISE e_mimes_exception;                                             
		 END IF;
		                  
    END IF; -- IF l_t_err_tbl.COUNT > 0
    
	l_dbg_msg := '300.03  Clearing table type variables.';
	write_log('Clearing table type variables.');
    l_t_err_tbl.DELETE; 
    l_t_trx_status_tbl.DELETE;      
      
    l_dbg_msg := '300.04 Getting validation transaction record error count for Instance ID: '||p_instance_id;
	write_log('Getting validation transaction record error count for Instance ID: '||p_instance_id);
    SELECT COUNT(*)
      INTO p_x_error_count
      FROM MROI_TT_TRX_STATUS   
     WHERE STATUS_CODE = MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_ERRED_STG
       AND INSTANCE_ID =  p_instance_id;          
EXCEPTION
	WHEN e_mimes_exception THEN
		p_x_mimes_error_code := l_x_mimes_error_code;
		p_x_mimes_error_msg := 'MIMES exception occurred when '||l_dbg_msg||' - '||l_x_mimes_error_msg;
	WHEN OTHERS THEN
		p_x_mroi_error_code := 1;
		p_x_mroi_error_msg := 'Other exception occurred when '||l_dbg_msg||' - '||SQLERRM;
END validate_records;
-- Procedure to write to the concurrent request log
PROCEDURE write_log(p_string VARCHAR2)
IS 
BEGIN
    fnd_file.put_line(FND_FILE.log, p_string);
    --dbms_output.put_line('Log: '||p_string);
END write_log;

-- Procedure to write to the concurrent request output
PROCEDURE write_output(p_string VARCHAR2)
IS 
BEGIN
    fnd_file.put_line(FND_FILE.output, p_string);
    --dbms_output.put_line('Output: '||p_string);
END write_output;
				
END MROI_AHL_ITEM_GROUP_CONV_PKG;