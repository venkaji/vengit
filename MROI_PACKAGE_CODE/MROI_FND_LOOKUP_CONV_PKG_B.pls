create or replace PACKAGE BODY MROI_FND_LOOKUP_CONV_PKG AS
 /* ***************************************************************************
 /*  Name:          MROI_FND_LOOKUP_CONV_PKG 
 /*  Object Type:   Package Body
 /*  Description:   Package Body for FND Lookup Conversion (MC Positions)
 /*
 /*  RICE Type:     Conversion
 /*  RICE ID:    	0007B
 /*
 /*  Change History:
 /*  Date          Name                           Ver      Modification
 /*  -----------   ----------------------         ----     -------------
 /*  06/05/2019    Rufo Calimlim                  1.0      Initial Creation
 /*  06/18/2019    Rufo Calimlim                  1.1      User Story IDs: 100, 101
 /*                                                        and 102 functionality
 /******************************************************************************/
 
 /* ***************************************************************************
 /*  Name:          main 
 /*  Object Type:   Procedure
 /*  Description:   This is the main conversion procedure registered as a 
 /*                 concurrent program
 /*
 /*  Calls:         stage_file_records, validate_records, load_records, write_log,
 /*                 MROI_TT_STATUS_UTIL_PKG, MROI_UTIL_PKG
 /*  Source:        MROI_FND_LOOKUP_CONV_PKG_PB.pls
 /*  Parameters:    p_x_errbuf - OUT parameter for concurrent program
 /*					p_x_retcode - OUT parameter for concurrent program
 /*					p_mode - IN parameter for Validate (V) or Load (L) mode
 /*					p_filename - IN parameter for conversion filename
 /*					p_lookup_type - IN parameter for Lookup Type
 /*					p_application - IN parameter for Application Name
 /*  Return values: N/A
 /*
 /*  Change History:
 /*  Date          Name                           Ver      Modification
 /*  -----------   ----------------------         ----     -------------
 /*  06/05/2019    Rufo Calimlim                  1.0      Initial Creation
 /******************************************************************************/
PROCEDURE main (
				p_x_errbuf OUT VARCHAR2,
				p_x_retcode OUT NUMBER,
				p_mode IN VARCHAR2,
				p_filename IN VARCHAR2,
				p_lookup_type IN VARCHAR2,
				p_application IN VARCHAR2
				)
IS
	l_dbg_msg			VARCHAR2(2000);
	l_instance_id		NUMBER := -1;
	l_user_name			VARCHAR2(100) := 'CONVERSION';
	l_resp_name			VARCHAR2(100) := 'MROI Conversion Responsibility';
	l_user_id			NUMBER;
	l_conc_req_id		NUMBER;
	l_curr_date_time	VARCHAR2(20) := TO_CHAR(sysdate, 'MMDDYYYYHH24MISS');
	l_conv_start		DATE;
	l_lookup_type		VARCHAR2(30);
	l_application		VARCHAR2(240);
	l_sql_str			VARCHAR2(1000);
	l_archive_filename	VARCHAR2(1000);
    l_bad_filename	    VARCHAR2(1000);
	
	l_x_total_txn_count NUMBER := 0;
	l_x_mroi_error_code NUMBER;
	l_x_mroi_error_msg	VARCHAR2(2000);
	l_x_mimes_error_code NUMBER;
	l_x_mimes_error_msg VARCHAR2(2000);
	l_x_error_count		NUMBER := 0;
	l_x_api_error_count	NUMBER := 0;
	
	e_mimes_exception	EXCEPTION;
	e_proc_exception	EXCEPTION;
	e_missing_params	EXCEPTION;
	e_bad_file			EXCEPTION;
BEGIN

	l_conv_start := TO_DATE(l_curr_date_time, 'MMDDYYYYHH24MISS');
	write_log('*************************************************************************');
	write_log('Conversion started '||TO_CHAR(l_conv_start, 'DD-MON-RRRR HH24:MI:SS'));
	write_log('Parameters Entered:');
	write_log('p_mode: '||p_mode);
	write_log('p_filename: '||p_filename);
	write_log('p_lookup_type: '||p_lookup_type);
	write_log('p_application: '||p_application);
	write_log('*************************************************************************');
	
	l_dbg_msg := '100.00 Check if parameters are populated.';
	
	-- Check if the Mode, Filename, Lookup Type, and Application Name  parameters are provided
	IF p_mode IS NULL OR p_filename IS NULL OR p_lookup_type IS NULL OR p_application IS NULL THEN
		write_log('Mode, Filename, Lookup Type, or Application Name parameters are not populated.');
		RAISE e_missing_params;
	END IF;
      
	l_conc_req_id := FND_GLOBAL.conc_request_id;
	
	l_dbg_msg := '100.00.01 Calling MROI_UTIL_PKG.initialize for user: '||l_user_name||' and Responsibility: '||l_resp_name;
    write_log('Calling MROI_UTIL_PKG.initialize for user: '||l_user_name||' and Responsibility: '||l_resp_name);
    IF NOT(MROI_UTIL_PKG.initialize(l_user_name, l_resp_name)) THEN
		RAISE e_proc_exception;
	END IF;

	l_user_id := FND_GLOBAL.user_id;
    
	-- Assign parameters to local variables for clarity
	l_lookup_type := p_lookup_type;
	l_application := p_application;
	
	l_dbg_msg := '100.01 Altering external table MROI_FND_LOOKUP_EXT.';
	write_log('Altering external table MROI_FND_LOOKUP_EXT with FILENAME as: '||p_filename);
	write_log('Altering external table MROI_FND_LOOKUP_EXT with BADFILE as: MROI_FND_LOOKUP_EXT_'||l_curr_date_time||'.bad');
	write_log('Altering external table MROI_FND_LOOKUP_EXT with LOGFILE as: MROI_FND_LOOKUP_EXT_'||l_curr_date_time||'.log');
	l_sql_str := 'ALTER TABLE XXMRO.MROI_FND_LOOKUP_EXT ACCESS PARAMETERS (RECORDS DELIMITED BY NEWLINE
				BADFILE MROI_MC_POS_CNV_ARCHIVE:''MROI_FND_LOOKUP_EXT_'||l_curr_date_time||'.bad'''||chr(10)||
				'LOGFILE MROI_MC_POS_CNV_ARCHIVE:''MROI_FND_LOOKUP_EXT_'||l_curr_date_time||'.log'''||chr(10)||
				'SKIP 1
				FIELDS CSV WITHOUT EMBEDDED
				lrtrim
				MISSING FIELD VALUES ARE NULL
				) LOCATION ('''||p_filename||''')';
				
	EXECUTE IMMEDIATE (l_sql_str);
	
	l_dbg_msg := '100.01.1 Selecting from external table MROI_FND_LOOKUP_EXT.';
	write_log('Selecting from external table MROI_FND_LOOKUP_EXT.');
	SELECT COUNT(*)
    INTO l_x_total_txn_count
    FROM MROI_FND_LOOKUP_EXT;
	
    l_bad_filename := 'MROI_FND_LOOKUP_EXT_'||l_curr_date_time||'.bad';
    l_dbg_msg := '100.01.2 Calling MROI_UTIL_PKG.file_exists to check if a BAD file was generated.';
	write_log('Calling MROI_UTIL_PKG.file_exists to check if a BAD file was generated.');
    IF MROI_UTIL_PKG.file_exists(l_bad_filename, 'MROI_MC_POS_CNV_ARCHIVE') THEN
        write_log('ERROR----> BAD File: '||l_bad_filename||' has been generated for this conversion run, please review the file and make any necessary corrections.');
		RAISE e_bad_file;
    END IF;
										
	l_dbg_msg := '100.02 Calling MROI_TT_STATUS_UTIL_PKG.initialize_instance.';
	write_log('Calling MROI_TT_STATUS_UTIL_PKG.initialize_instance.');
    write_log('l_conc_req_id '||l_conc_req_id);
	MROI_TT_STATUS_UTIL_PKG.initialize_instance(
							  p_tech_id          => l_conc_req_id
                             ,p_tech_type        => 'CONC_REQ'
                             ,p_process_id       => MROI_TT_STATUS_UTIL_PKG.get_process_id('MROI_MC_POS_CNV')
                             ,p_file_name        => p_filename
                             ,p_start_date       => sysdate                             
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
				p_lookup_type 			=> l_lookup_type,
				p_application 			=> l_application,
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
                        ,p_file_directory  => 'MROI_MC_POS_CNV'
                        ,p_file_archive_directory => 'MROI_MC_POS_CNV_ARCHIVE'                       
                        ,p_archive_file_name_list => l_archive_filename
                        ,p_error_msg => l_x_mroi_error_msg);
                        
    write_log('Archive file: '||l_archive_filename);
                        
    IF l_x_mroi_error_msg IS NOT NULL THEN
        RAISE e_proc_exception;
	END IF;
    
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
    IF p_mode = 'L' OR p_mode = 'V' THEN
    
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

		l_dbg_msg := '100.09 Calling load_records procedure.';
        write_log('Calling load_records procedure.');
		load_records (
					p_instance_id 			=> l_instance_id,
					p_user_id 				=> l_user_id,
					p_api_mode 				=> p_mode,
					p_x_api_error_count 	=> l_x_api_error_count,
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
        
        write_log('Number of records with API errors: '||l_x_api_error_count);
        
        l_dbg_msg := '100.10 Calling MROI_TT_STATUS_UTIL_PKG.update_instance_status to set status to '||MROI_TT_STATUS_UTIL_PKG.G_INSTCODE_RECS_LOADED_OIT_API;
        write_log('Calling MROI_TT_STATUS_UTIL_PKG.update_instance_status to set status to '||MROI_TT_STATUS_UTIL_PKG.G_INSTCODE_RECS_LOADED_OIT_API);
        MROI_TT_STATUS_UTIL_PKG.update_instance_status(
                                     p_instance_id         => l_instance_id
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
                                    
        IF l_x_mimes_error_code = 1 THEN                                            
            RAISE e_mimes_exception;                                             
        END IF; 
    END IF; -- IF p_mode = 'L' OR p_mode = 'V'
       
	l_dbg_msg := '100.11 Calling generate_error_report procedure.';
	write_log('Calling generate_error_report procedure.');
	generate_error_report (
				p_instance_id 			=> l_instance_id,
				p_execute_mode 			=> p_mode,
				p_start_time 			=> l_conv_start,
				p_cnv_filename 			=> p_filename,
				p_lookup_type			=> p_lookup_type,
				p_total_txn_count 		=> l_x_total_txn_count,
				p_val_error_count 		=> l_x_error_count,
				p_api_error_count 		=> l_x_api_error_count,
				p_x_mroi_error_code 	=> l_x_mroi_error_code,
				p_x_mroi_error_msg 		=> l_x_mroi_error_msg
				);
				
	IF l_x_mroi_error_code = 1 THEN
		RAISE e_proc_exception;
	END IF;
	
	l_dbg_msg := '100.12 Calling MROI_TT_STATUS_UTIL_PKG.update_instance_status to set status to '||MROI_TT_STATUS_UTIL_PKG.G_INSTCODE_COMPLETED;
	write_log('Calling MROI_TT_STATUS_UTIL_PKG.update_instance_status to set status to '||MROI_TT_STATUS_UTIL_PKG.G_INSTCODE_COMPLETED);
	MROI_TT_STATUS_UTIL_PKG.update_instance_status(
								 p_instance_id         => l_instance_id
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
								
	IF l_x_mimes_error_code = 1 THEN                                            
		RAISE e_mimes_exception;                                             
	END IF;
    
	write_log('*************************************************************************');
    write_log('Number of records staged: '||l_x_total_txn_count);
    write_log('Number of validation error records: '||l_x_error_count);
    write_log('Number of API error records: '||l_x_api_error_count);
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
		p_x_errbuf := 'Mode, Filename, Lookup Type, or Application Name parameters are not populated.';
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
 /*  Source:        MROI_FND_LOOKUP_CONV_PKG_PB.pls
 /*  Parameters:    p_instance_id - IN parameter for MIMES instance ID
 /*                 p_user_id - IN parameter for FND User ID
 /*                 p_lookup_type - IN parameter for Lookup Type
 /*                 p_application - IN parameter for Application Name
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
 /*  06/05/2019    Rufo Calimlim                  1.0      Initial Creation
 /******************************************************************************/				
PROCEDURE stage_file_records (
				p_instance_id IN NUMBER,
				p_user_id IN NUMBER,
				p_lookup_type IN VARCHAR2,
				p_application IN VARCHAR2,
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
	
	l_dbg_msg := '200.01 Inserting records into MROI_FND_LOOKUP_HDR_STG table.';
	write_log('Inserting records into MROI_FND_LOOKUP_HDR_STG table.');

	INSERT INTO MROI_FND_LOOKUP_HDR_STG (
		RECORD_HEADER_ID,
		INSTANCE_ID,
		LOOKUP_TYPE,
		LOOKUP_CODE,
		MEANING,
		DESCRIPTION,
		APPLICATION_NAME,
		ENABLED_FLAG,
		START_DATE_ACTIVE,
		CREATION_DATE,
		LAST_UPDATE_DATE,
		CREATED_BY,
		LAST_UPDATED_BY) 
		SELECT	MROI_FND_LOOKUP_HDR_STG_S.nextval,
				p_instance_id,
				p_lookup_type,
				code,
				meaning,
				description,
				p_application,
				'Y',
				sysdate,
				sysdate,
				sysdate,
				p_user_id,
				p_user_id
		FROM MROI_FND_LOOKUP_EXT;
	
	l_x_total_txn_count := SQL%ROWCOUNT;
	write_log('Number of records inserted into MROI_FND_LOOKUP_HDR_STG table: '||l_x_total_txn_count);
	
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
 /*  Source:        MROI_FND_LOOKUP_CONV_PKG_PB.pls
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
 /*  06/05/2019    Rufo Calimlim                  1.0      Initial Creation
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
        SELECT MROI_FND_LOOKUP_ERR_S.nextval AS error_id, rec.instance_id, rec.transaction_id, rec.record_level_id, rec.record_level_type, rec.error_field, rec.error_code, rec.error_attribute1, rec.error_attribute2, rec.error_attribute3, rec.error_attribute4
        FROM (
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'APPLICATION_NAME' as error_field, 13000 as error_code,
            '' as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_FND_LOOKUP_HDR_STG a
            WHERE a.application_name IS NULL
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'LOOKUP_TYPE' as error_field, 13000 as error_code,
            '' as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_FND_LOOKUP_HDR_STG a
            WHERE a.lookup_type IS NULL
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'LOOKUP_CODE' as error_field, 13000 as error_code,
            '' as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_FND_LOOKUP_HDR_STG a
            WHERE a.lookup_code IS NULL
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'MEANING' as error_field, 13000 as error_code,
            '' as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_FND_LOOKUP_HDR_STG a
            WHERE a.meaning IS NULL
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'APPLICATION_NAME' as error_field, 10001 as error_code,
            a.application_name as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_FND_LOOKUP_HDR_STG a
            WHERE NOT EXISTS (SELECT 'x' FROM FND_APPLICATION_TL b WHERE b.application_name = a.application_name AND b.language = userenv('lang'))
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'LOOKUP_CODE' as error_field, 15001 as error_code,
            'LOOKUP_CODE value includes invalid characters' as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_FND_LOOKUP_HDR_STG a
            WHERE regexp_like(a.lookup_code, '[^A-Za-z0-9._]')
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'LOOKUP_TYPE' as error_field, 10001 as error_code,
            a.lookup_type as error_attribute1, '' as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_FND_LOOKUP_HDR_STG a
            WHERE NOT EXISTS (SELECT 'x' FROM FND_APPLICATION_TL b, FND_LOOKUP_TYPES c
            WHERE b.application_name = a.application_name AND b.language = userenv('lang')
            AND c.application_id = b.application_id
            AND c.lookup_type = a.lookup_type)
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'LOOKUP_CODE' as error_field, 16001 as error_code,
            a.lookup_code as error_attribute1, a.lookup_type as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_FND_LOOKUP_HDR_STG a
            WHERE EXISTS (SELECT 'x' FROM FND_LOOKUP_VALUES b
            WHERE b.lookup_type = a.lookup_type
            AND b.lookup_code = a.lookup_code
            AND b.language = userenv('lang'))
            UNION ALL
            SELECT a.instance_id, a.transaction_id, a.record_header_id AS record_level_id, 'HEADER' as record_level_type, 'MEANING' as error_field, 16001 as error_code,
            a.meaning as error_attribute1, a.lookup_type as error_attribute2, '' as error_attribute3, '' as error_attribute4
            FROM MROI_FND_LOOKUP_HDR_STG a
            WHERE EXISTS (SELECT 'x' FROM FND_LOOKUP_VALUES b
            WHERE b.lookup_type = a.lookup_type
            AND b.meaning = a.meaning
            AND b.language = userenv('lang'))) rec
        WHERE rec.instance_id = p_c_instance_id;
	
BEGIN
	p_x_mroi_error_code := 0;
	p_x_mroi_error_msg := NULL;
	p_x_mimes_error_code := 0;
	p_x_mimes_error_msg := NULL;
	p_x_error_count := 0;
    
    l_dbg_msg := '300.00 Opening cursor FOR LOOP for c_invalid_values using Instance ID: '||p_instance_id;
    write_log('Opening cursor FOR LOOP for c_invalid_values using Instance ID: '||p_instance_id);
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
   
		-- Call MIMES API to insert collection of erred records into MROI_FND_LOOKUP_ERR table
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

 /* ***************************************************************************
 /*  Name:          load_records 
 /*  Object Type:   Procedure
 /*  Description:   Procedure to invoke the FND_LOOKUP_VALUES_PKG API
 /*                 for successfully validated records for a specific Instance ID 
 /*                 and invoke MIMES for each transaction to update the status
 /*
 /*  Calls:         MROI_TT_ERROR_UTIL_PKG, MROI_TT_STATUS_UTIL_PKG, FND_LOOKUP_VALUES_PKG
 /*  Source:        MROI_FND_LOOKUP_CONV_PKG_PB.pls
 /*  Parameters:    p_instance_id - IN parameter for MIMES instance ID
 /*                 p_user_id - IN parameter for FND User ID
 /*                 p_api_mode - IN parameter for API to perform validation or actual load
 /*                 p_x_api_error_count - OUT parameter for number of API error records
 /*                 p_x_mroi_error_code - OUT parameter for error code
 /*                 p_x_mroi_error_msg - OUT parameter for error message
 /*                 p_x_mimes_error_code - OUT parameter for MIMES error code
 /*                 p_x_mimes_error_msg - OUT parameter for MIMES error message
 /*  Return values: N/A
 /*
 /*  Change History:
 /*  Date          Name                           Ver      Modification
 /*  -----------   ----------------------         ----     -------------
 /*  06/18/2019    Rufo Calimlim                  1.0      Initial Creation
 /******************************************************************************/		
PROCEDURE load_records (
				p_instance_id IN NUMBER,
				p_user_id IN NUMBER,
				p_api_mode IN VARCHAR2,
				p_x_api_error_count OUT NUMBER,
				p_x_mroi_error_code OUT NUMBER,
				p_x_mroi_error_msg OUT VARCHAR2,
				p_x_mimes_error_code OUT NUMBER,
				p_x_mimes_error_msg OUT VARCHAR2
				)
IS
	PRAGMA AUTONOMOUS_TRANSACTION;
	
	l_dbg_msg				VARCHAR2(2000);
	l_x_api_error_count		NUMBER := 0;
	l_x_mimes_error_code 	NUMBER := 0;
	l_x_mimes_error_msg 	VARCHAR2(2000);
	e_mimes_exception		EXCEPTION;
	
	l_rowid 				VARCHAR2(64);
	l_api_flag				VARCHAR2(1);
	l_view_application_id	FND_LOOKUP_VALUES.VIEW_APPLICATION_ID%TYPE;
	l_lookup_exists			VARCHAR2(1);
	
	l_ora_error_code		VARCHAR2(30);
	l_ora_error_msg			VARCHAR2(4000);
	l_error_msg				VARCHAR2(2000);
	
	CURSOR c_lookup_values (p_c_instance_id NUMBER, p_c_status_code NUMBER) IS
		SELECT stg.* 
		  FROM MROI_FND_LOOKUP_HDR_STG stg, MROI_TT_TRX_STATUS txn
		 WHERE stg.instance_id=p_c_instance_id
		   AND stg.instance_id=txn.instance_id
		   AND stg.transaction_id=txn.transaction_id
		   AND txn.status_code = p_c_status_code;
BEGIN

	l_dbg_msg := '500.00 Opening cursor FOR LOOP for c_lookup_values using Instance ID: '||p_instance_id||' and Transaction Status: '||MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_LOADED_STG;
    write_log('Opening cursor FOR LOOP for c_lookup_values using Instance ID: '||p_instance_id||' and Transaction Status: '||MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_LOADED_STG);
	FOR r_lookup_values IN c_lookup_values (p_instance_id, MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_LOADED_STG) LOOP
		-- Reset variables
		l_error_msg := NULL;
		l_rowid := NULL;
		l_view_application_id := NULL;
		l_api_flag := NULL;
		l_lookup_exists	:= NULL;
		l_ora_error_code := NULL;
		l_ora_error_msg := NULL;
		
		l_dbg_msg := '500.01 Looking up View Application ID for Application: '||r_lookup_values.application_name||' and Lookup Type: '||r_lookup_values.lookup_type;
		write_log('Looking up View Application ID for Application: '||r_lookup_values.application_name||' and Lookup Type: '||r_lookup_values.lookup_type);
		BEGIN
		
			SELECT flt.view_application_id
			  INTO l_view_application_id
			  FROM FND_APPLICATION_TL fat, FND_LOOKUP_TYPES flt
			 WHERE fat.application_name = r_lookup_values.application_name
			   AND fat.language = USERENV('lang')
			   AND flt.lookup_type = r_lookup_values.lookup_type
			   AND flt.application_id = fat.application_id;
			   
		EXCEPTION WHEN NO_DATA_FOUND THEN
			l_error_msg := l_error_msg||' Could not derive View Application ID for Application: '||r_lookup_values.application_name||' and Lookup Type: '||r_lookup_values.lookup_type;
			l_x_api_error_count := l_x_api_error_count + 1;
			
			l_dbg_msg := '500.02 Calling MROI_TT_ERROR_UTIL_PKG.log_error_single for Transaction ID: '||r_lookup_values.transaction_id;
			write_log('Calling MROI_TT_ERROR_UTIL_PKG.log_error_single for Transaction ID: '||r_lookup_values.transaction_id);	
			MROI_TT_ERROR_UTIL_PKG.log_error_single(
						  p_instance_id          => p_instance_id
                         ,p_transaction_id       => r_lookup_values.transaction_id
                         ,p_record_level_id      => r_lookup_values.record_header_id
                         ,p_record_level_type    => 'HEADER'
                         ,p_error_code           => 15000
                         ,p_error_field          => 'LOOKUP_TYPE'
                         ,p_error_attribute1     => 'View Application ID for '||r_lookup_values.lookup_type
                         ,p_error_attribute2     => NULL
                         ,p_error_attribute3     => NULL
                         ,p_error_attribute4     => NULL
                         ,p_ora_err_code         => NULL
                         ,p_ora_err_msg          => NULL
                         ,p_user_id              => p_user_id
                         ,p_x_mimes_err_code     => l_x_mimes_error_code
                         ,p_x_mimes_err_msg      => l_x_mimes_error_msg);
			
			IF l_x_mimes_error_code = 1 THEN                                            
				RAISE e_mimes_exception;                                             
			END IF;
			
			l_dbg_msg := '500.03 Calling MROI_TT_STATUS_UTIL_PKG.update_trx_status_single for Transaction ID: '||r_lookup_values.transaction_id||' and Status: '||MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_ERRED_OIT_API;
			write_log('Calling MROI_TT_STATUS_UTIL_PKG.update_trx_status_single for Transaction ID: '||r_lookup_values.transaction_id||' and Status: '||MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_ERRED_OIT_API);
			MROI_TT_STATUS_UTIL_PKG.update_trx_status_single(
									p_transaction_id        => r_lookup_values.transaction_id
                                   ,p_status_code           => MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_ERRED_OIT_API 
                                   ,p_reprocess_instance_id => NULL
                                   ,p_reject_reason         => NULL
                                   ,p_remarks               => NULL
                                   ,p_oit_reject_reason     => NULL
                                   ,p_oit_remarks           => NULL 
                                   ,p_user_id               => p_user_id                                                 
                                   ,p_x_mimes_err_code      => l_x_mimes_error_code
                                   ,p_x_mimes_err_msg       => l_x_mimes_error_msg);
								   
			IF l_x_mimes_error_code = 1 THEN                                            
				RAISE e_mimes_exception;                                             
			END IF;
		END;
		
		-- Call API if no errors and View Application ID is found
		IF l_error_msg IS NULL THEN

			l_dbg_msg := '500.04 Calling FND_LOOKUP_VALUES_PKG.INSERT_ROW for Transaction ID: '||r_lookup_values.transaction_id;
			write_log('Calling FND_LOOKUP_VALUES_PKG.INSERT_ROW for Transaction ID: '||r_lookup_values.transaction_id);
			BEGIN
				FND_LOOKUP_VALUES_PKG.INSERT_ROW (
						 X_ROWID => l_rowid,
						 X_LOOKUP_TYPE => r_lookup_values.lookup_type,
						 X_SECURITY_GROUP_ID => r_lookup_values.security_group_id,
						 X_VIEW_APPLICATION_ID => l_view_application_id,
						 X_LOOKUP_CODE => r_lookup_values.lookup_code,
						 X_TAG => NULL,
						 X_ATTRIBUTE_CATEGORY => r_lookup_values.attribute_category,
						 X_ATTRIBUTE1 => r_lookup_values.attribute1,
						 X_ATTRIBUTE2 => r_lookup_values.attribute2,
						 X_ATTRIBUTE3 => r_lookup_values.attribute3,
						 X_ATTRIBUTE4 => r_lookup_values.attribute4,
						 X_ENABLED_FLAG => r_lookup_values.enabled_flag,
						 X_START_DATE_ACTIVE => r_lookup_values.start_date_active,
						 X_END_DATE_ACTIVE => r_lookup_values.end_date_active,
						 X_TERRITORY_CODE => NULL,
						 X_ATTRIBUTE5 => r_lookup_values.attribute5,
						 X_ATTRIBUTE6 => r_lookup_values.attribute6,
						 X_ATTRIBUTE7 => r_lookup_values.attribute7,
						 X_ATTRIBUTE8 => r_lookup_values.attribute8,
						 X_ATTRIBUTE9 => r_lookup_values.attribute9,
						 X_ATTRIBUTE10 => r_lookup_values.attribute10,
						 X_ATTRIBUTE11 => r_lookup_values.attribute11,
						 X_ATTRIBUTE12 => r_lookup_values.attribute12,
						 X_ATTRIBUTE13 => r_lookup_values.attribute13,
						 X_ATTRIBUTE14 => r_lookup_values.attribute14,
						 X_ATTRIBUTE15 => r_lookup_values.attribute15,
						 X_MEANING => r_lookup_values.meaning,
						 X_DESCRIPTION => r_lookup_values.description,
						 X_CREATION_DATE => sysdate,
						 X_CREATED_BY => p_user_id,
						 X_LAST_UPDATE_DATE => sysdate,
						 X_LAST_UPDATED_BY => p_user_id,
						 X_LAST_UPDATE_LOGIN => FND_GLOBAL.LOGIN_ID);
						 
				-- No API error set flag to S
				l_api_flag := 'S';
				
			EXCEPTION WHEN OTHERS THEN
				l_dbg_msg := '500.05 Error calling FND_LOOKUP_VALUES_PKG.INSERT_ROW for Transaction ID: '||r_lookup_values.transaction_id;
				write_log('Error calling FND_LOOKUP_VALUES_PKG.INSERT_ROW for Transaction ID: '||r_lookup_values.transaction_id);
				
				l_ora_error_code := TO_CHAR(SQLCODE);
				l_ora_error_msg	:= SUBSTR(SQLERRM, 1, 4000);
				
				-- API error set flag to E
				l_api_flag := 'E';
				
				ROLLBACK;
			END;

			-- If API returns success then update MIMES transaction status
			IF l_api_flag = 'S' THEN
					
				l_dbg_msg := '500.06 Calling MROI_TT_STATUS_UTIL_PKG.update_trx_status_single for Transaction ID: '||r_lookup_values.transaction_id||' and Status: '||MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_LOADED_OIT_API;
				write_log('Calling MROI_TT_STATUS_UTIL_PKG.update_trx_status_single for Transaction ID: '||r_lookup_values.transaction_id||' and Status: '||MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_LOADED_OIT_API);
				MROI_TT_STATUS_UTIL_PKG.update_trx_status_single(
										p_transaction_id        => r_lookup_values.transaction_id
									   ,p_status_code           => MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_LOADED_OIT_API 
									   ,p_reprocess_instance_id => NULL
									   ,p_reject_reason         => NULL
									   ,p_remarks               => NULL
									   ,p_oit_reject_reason     => NULL
									   ,p_oit_remarks           => NULL 
									   ,p_user_id               => p_user_id                                                 
									   ,p_x_mimes_err_code      => l_x_mimes_error_code
									   ,p_x_mimes_err_msg       => l_x_mimes_error_msg);
									   
				IF l_x_mimes_error_code = 1 THEN                                            
					RAISE e_mimes_exception;                                             
				END IF;
				
				-- If Mode is L issue COMMIT and check if record exists in MROI
				IF p_api_mode = 'L' THEN
					COMMIT;
					
					l_dbg_msg := '500.07 Checking if Lookup Value exists for Lookup Type: '||r_lookup_values.lookup_type||' Code: '||r_lookup_values.lookup_code||' and Meaning: '||r_lookup_values.meaning;
					write_log('Checking if Lookup Value exists for Lookup Type: '||r_lookup_values.lookup_type||' Code: '||r_lookup_values.lookup_code||' and Meaning: '||r_lookup_values.meaning);
					BEGIN
		
						SELECT 'Y'
						  INTO l_lookup_exists
						  FROM FND_LOOKUP_VALUES flv
						 WHERE flv.lookup_type = r_lookup_values.lookup_type
						   AND flv.lookup_code = r_lookup_values.lookup_code
						   AND flv.meaning = r_lookup_values.meaning
						   AND flv.language = USERENV('lang');
								
						l_dbg_msg := '500.08 Calling MROI_TT_STATUS_UTIL_PKG.update_trx_status_single for Transaction ID: '||r_lookup_values.transaction_id||' and Status: '||MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_LOADED_EBS;
						write_log('Calling MROI_TT_STATUS_UTIL_PKG.update_trx_status_single for Transaction ID: '||r_lookup_values.transaction_id||' and Status: '||MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_LOADED_EBS);
						MROI_TT_STATUS_UTIL_PKG.update_trx_status_single(
												p_transaction_id        => r_lookup_values.transaction_id
											   ,p_status_code           => MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_LOADED_EBS 
											   ,p_reprocess_instance_id => NULL
											   ,p_reject_reason         => NULL
											   ,p_remarks               => NULL
											   ,p_oit_reject_reason     => NULL
											   ,p_oit_remarks           => NULL 
											   ,p_user_id               => p_user_id                                                 
											   ,p_x_mimes_err_code      => l_x_mimes_error_code
											   ,p_x_mimes_err_msg       => l_x_mimes_error_msg);
											   
						IF l_x_mimes_error_code = 1 THEN                                            
							RAISE e_mimes_exception;                                             
						END IF;
						
					EXCEPTION WHEN NO_DATA_FOUND THEN
						l_x_api_error_count := l_x_api_error_count + 1;
						l_dbg_msg := '500.08 Calling MROI_TT_STATUS_UTIL_PKG.update_trx_status_single for Transaction ID: '||r_lookup_values.transaction_id||' and Status: '||MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_MISSING_EBS;
						write_log('Calling MROI_TT_STATUS_UTIL_PKG.update_trx_status_single for Transaction ID: '||r_lookup_values.transaction_id||' and Status: '||MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_MISSING_EBS);
						MROI_TT_STATUS_UTIL_PKG.update_trx_status_single(
												p_transaction_id        => r_lookup_values.transaction_id
											   ,p_status_code           => MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_MISSING_EBS 
											   ,p_reprocess_instance_id => NULL
											   ,p_reject_reason         => NULL
											   ,p_remarks               => NULL
											   ,p_oit_reject_reason     => NULL
											   ,p_oit_remarks           => NULL 
											   ,p_user_id               => p_user_id                                                 
											   ,p_x_mimes_err_code      => l_x_mimes_error_code
											   ,p_x_mimes_err_msg       => l_x_mimes_error_msg);
											   
						IF l_x_mimes_error_code = 1 THEN                                            
							RAISE e_mimes_exception;                                             
						END IF;
						
					END;
					
				ELSE
					ROLLBACK;
				END IF; -- IF p_api_mode = 'L' THEN
				
			ELSIF l_api_flag = 'E' THEN
				l_x_api_error_count := l_x_api_error_count + 1;
				
				l_dbg_msg := '500.09 Calling MROI_TT_ERROR_UTIL_PKG.log_error_single for Transaction ID: '||r_lookup_values.transaction_id;
				write_log('Calling MROI_TT_ERROR_UTIL_PKG.log_error_single for Transaction ID: '||r_lookup_values.transaction_id);	
				MROI_TT_ERROR_UTIL_PKG.log_error_single(
							  p_instance_id          => p_instance_id
							 ,p_transaction_id       => r_lookup_values.transaction_id
							 ,p_record_level_id      => r_lookup_values.record_header_id
							 ,p_record_level_type    => 'HEADER'
							 ,p_error_code           => 00000
							 ,p_error_field          => 'LOOKUP_CODE'
							 ,p_error_attribute1     => NULL
							 ,p_error_attribute2     => NULL
							 ,p_error_attribute3     => NULL
							 ,p_error_attribute4     => NULL
							 ,p_ora_err_code         => l_ora_error_code
							 ,p_ora_err_msg          => l_ora_error_msg
							 ,p_user_id              => p_user_id
							 ,p_x_mimes_err_code     => l_x_mimes_error_code
							 ,p_x_mimes_err_msg      => l_x_mimes_error_msg);
				
				IF l_x_mimes_error_code = 1 THEN                                            
					RAISE e_mimes_exception;                                             
				END IF;
				
				l_dbg_msg := '500.10 Calling MROI_TT_STATUS_UTIL_PKG.update_trx_status_single for Transaction ID: '||r_lookup_values.transaction_id||' and Status: '||MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_ERRED_OIT_API;
				write_log('Calling MROI_TT_STATUS_UTIL_PKG.update_trx_status_single for Transaction ID: '||r_lookup_values.transaction_id||' and Status: '||MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_ERRED_OIT_API);
				MROI_TT_STATUS_UTIL_PKG.update_trx_status_single(
										p_transaction_id        => r_lookup_values.transaction_id
									   ,p_status_code           => MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_ERRED_OIT_API 
									   ,p_reprocess_instance_id => NULL
									   ,p_reject_reason         => NULL
									   ,p_remarks               => NULL
									   ,p_oit_reject_reason     => NULL
									   ,p_oit_remarks           => NULL 
									   ,p_user_id               => p_user_id                                                 
									   ,p_x_mimes_err_code      => l_x_mimes_error_code
									   ,p_x_mimes_err_msg       => l_x_mimes_error_msg);
									   
				IF l_x_mimes_error_code = 1 THEN                                            
					RAISE e_mimes_exception;                                             
				END IF;
			END IF; -- IF l_api_flag = 'S'
    
		END IF; -- IF l_error_msg IS NULL THEN
	
	END LOOP; -- FOR r_lookup_values IN c_lookup_values (p_instance_id, MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_LOADED_STG) LOOP
	
	p_x_mroi_error_code := 0;
	p_x_mroi_error_msg := NULL;
	p_x_mimes_error_code := 0;
	p_x_mimes_error_msg := NULL;
	p_x_api_error_count := l_x_api_error_count;
	
EXCEPTION
	WHEN e_mimes_exception THEN
		p_x_mimes_error_code := l_x_mimes_error_code;
		p_x_mimes_error_msg := 'MIMES exception occurred when '||l_dbg_msg||' - '||l_x_mimes_error_msg;
	WHEN OTHERS THEN
		p_x_mroi_error_code := 1;
		p_x_mroi_error_msg := 'Other exception occurred when '||l_dbg_msg||' - '||SQLERRM;
END load_records;

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

 /* ***************************************************************************
 /*  Name:          generate_error_report 
 /*  Object Type:   Procedure
 /*  Description:   Procedure to write the error records for the Conversion 
 /*                 Status Report
 /*
 /*  Calls:         MROI_TT_ERROR_UTIL_PKG, write_output
 /*  Source:        MROI_FND_LOOKUP_CONV_PKG_PB.pls
 /*  Parameters:    p_instance_id - IN parameter for MIMES instance ID
 /*					p_execute_mode - IN parameter for Execute Mode
 /*					p_start_time - IN parameter for Conversion Start Time
 /*					p_cnv_filename - IN parameter for Conversion Filename
 /*					p_lookup_type - IN parameter for Lookup Type
 /*					p_total_txn_count - IN parameter for Total Records Processed
 /*					p_val_error_count - IN parameter for Total Validation Errors
 /*					p_api_error_count - IN parameter for Total API Errors
 /*                 p_x_mroi_error_code - OUT parameter for error code
 /*                 p_x_mroi_error_msg - OUT parameter for error message
 /*  Return values: N/A
 /*
 /*  Change History:
 /*  Date          Name                           Ver      Modification
 /*  -----------   ----------------------         ----     -------------
 /*  06/18/2019    Rufo Calimlim                  1.0      Initial Creation
 /******************************************************************************/	
PROCEDURE generate_error_report(
				p_instance_id IN NUMBER,
				p_execute_mode IN VARCHAR2,
				p_start_time IN DATE,
				p_cnv_filename IN VARCHAR2,
				p_lookup_type IN VARCHAR2,
				p_total_txn_count IN NUMBER,
				p_val_error_count IN NUMBER,
				p_api_error_count IN NUMBER,
				p_x_mroi_error_code OUT NUMBER,
				p_x_mroi_error_msg OUT VARCHAR2)
IS
    CURSOR c_erred_recs(p_c_instance_id NUMBER, p_c_status_code NUMBER) IS
        SELECT stg.transaction_id, stg.lookup_type 
              ,stg.lookup_code, stg.meaning 
              ,stg.description
              ,err.error_code, err.error_field, err.error_attribute1
              ,err.error_attribute2, err.error_attribute3, err.error_attribute4
              ,err.ora_err_code,err.ora_err_msg
        FROM MROI_FND_LOOKUP_ERR err,
             MROI_FND_LOOKUP_HDR_STG stg,
             MROI_TT_TRX_STATUS txn
        WHERE 	err.record_level_type = 'HEADER'
		AND 	err.record_level_id = stg.record_header_id
        AND  	stg.instance_id = p_c_instance_id
        AND     stg.transaction_id = txn.transaction_id
        AND     err.transaction_id = txn.transaction_id
        AND     txn.status_code = p_c_status_code
        ORDER BY stg.transaction_id;
		
	l_dbg_msg 	VARCHAR2(2000);
	l_msg		VARCHAR2(2000);
BEGIN
	l_dbg_msg := '400.00 Generating Conversion Status Report for Instance ID: '||p_instance_id;
	write_log('Generating Conversion Status Report for Instance ID: '||p_instance_id);
	
	-- Writing Conversion Status Report information
    write_output('------------------------------RICE ID 0007B - FND Lookup Values Conversion - Conversion Status Report-----------------------------');
    write_output('Start Time: '||TO_CHAR(p_start_time, 'DD-MON-RRRR HH24:MI:SS'));
    write_output('End Time: '||TO_CHAR(sysdate, 'DD-MON-RRRR HH24:MI:SS'));
    write_output('Execute Mode: '||p_execute_mode);
    write_output('Name of File Processed: '||p_cnv_filename);
    write_output('Total Records Selected for Processing: '||p_total_txn_count);
    write_output('Total Records Successfully Processed: '||(p_total_txn_count - p_val_error_count - p_api_error_count));
    write_output('Total Records Failed Initial Validation: '||p_val_error_count);
    write_output('Total Records Failed API Process: '||p_api_error_count);
    
    -- Write FND Lookup Value count only if L was selected for Execute Mode
    IF p_execute_mode = 'L' THEN
        write_output('Total Number of '||p_lookup_type||' Lookup Values Created: '||(p_total_txn_count - p_val_error_count - p_api_error_count));
    END IF; -- IF p_execute_mode = 'L' THEN
    
    write_output('----------------------------------------------------------------------------------------------------------------------------------');
    write_output('The following records failed during conversion processsing:');
	
    write_output(chr(10)||'Records that Failed Validation:');
	l_dbg_msg := '400.01 Generating Header row for records that failed validation for Instance ID: '||p_instance_id;
	write_log('Generating Header row for records that failed validation for Instance ID: '||p_instance_id);
    l_msg := 'TRANSACTION_ID, LOOKUP_TYPE, CODE, MEANING, DESCRIPTION, ERROR'; 
	write_output(l_msg);
	
	l_dbg_msg := '400.02 Looping through c_erred_recs cursor to write validation errors to the concurrent request output for Instance ID: '||p_instance_id; 
	write_log('Looping through c_erred_recs cursor to write validation errors to the concurrent request output for Instance ID: '||p_instance_id);
    FOR r_erred_recs IN c_erred_recs(p_instance_id, MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_ERRED_STG) LOOP             
        l_msg := r_erred_recs.transaction_id||', '||r_erred_recs.lookup_type||', '||r_erred_recs.lookup_code||', '||
                  r_erred_recs.meaning||', '||r_erred_recs.description||', '||
                  MROI_TT_ERROR_UTIL_PKG.GET_ERROR_MSG(r_erred_recs.error_code,
													MROI_TT_STATUS_UTIL_PKG.get_process_id('DEFAULT'),
													r_erred_recs.error_field,
													r_erred_recs.error_attribute1, 
                                                    r_erred_recs.error_attribute2,
													r_erred_recs.error_attribute3, 
                                                    r_erred_recs.error_attribute4,
													r_erred_recs.ora_err_code,
													r_erred_recs.ora_err_msg);            
		write_output(l_msg);
    END LOOP;
	
    write_output('----------------------------------------------------------------------------------------------------------------------------------');
    
    write_output(chr(10)||'Records that Failed API Call:');
	l_dbg_msg := '400.03 Generating Header row for records that failed API call for Instance ID: '||p_instance_id;
	write_log('Generating Header row for records that failed API call for Instance ID: '||p_instance_id);
    l_msg := 'TRANSACTION_ID, LOOKUP_TYPE, CODE, MEANING, DESCRIPTION, ERROR'; 
	write_output(l_msg);
	
	l_dbg_msg := '400.04 Looping through c_erred_recs cursor to write API errors to the concurrent request output for Instance ID: '||p_instance_id; 
	write_log('Looping through c_erred_recs cursor to write API errors to the concurrent request output for Instance ID: '||p_instance_id);
    FOR r_erred_recs IN c_erred_recs(p_instance_id, MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_ERRED_OIT_API) LOOP             
        l_msg := r_erred_recs.transaction_id||', '||r_erred_recs.lookup_type||', '||r_erred_recs.lookup_code||', '||
                  r_erred_recs.meaning||', '||r_erred_recs.description||', '||
                  MROI_TT_ERROR_UTIL_PKG.GET_ERROR_MSG(r_erred_recs.error_code,
													MROI_TT_STATUS_UTIL_PKG.get_process_id('DEFAULT'),
													r_erred_recs.error_field,
													r_erred_recs.error_attribute1, 
                                                    r_erred_recs.error_attribute2,
													r_erred_recs.error_attribute3, 
                                                    r_erred_recs.error_attribute4,
													r_erred_recs.ora_err_code,
													r_erred_recs.ora_err_msg);            
		write_output(l_msg);
    END LOOP;
    
EXCEPTION         
    WHEN OTHERS THEN         
		p_x_mroi_error_code := 1;
		p_x_mroi_error_msg := 'Other exception occurred when '||l_dbg_msg||' - '||SQLERRM;
END generate_error_report;
				
END MROI_FND_LOOKUP_CONV_PKG;