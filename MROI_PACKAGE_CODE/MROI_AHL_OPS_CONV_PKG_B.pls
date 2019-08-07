create or replace PACKAGE BODY mroi_ahl_ops_conv_pkg
 AS       
    /* ***************************************************************************
 /*  Name:          MROI_AHL_OPS_CONV_PKG 
 /*  Object Type:   Package Body
 /*  Description:   Package Body for AHL Operation Conversion
 /*
 /*  RICE Type:     Conversion
 /*  RICE ID:    	0006
 /*
 /*  Change History:
 /*  Date          Name                           Ver      Modification
 /*  -----------   ----------------------         ----     -------------
 /*  06/05/2019    Trey Hernandez                 1.0      Initial Creation
 /*  06/18/2019    Yu Jian Lao                    1.1      Updated for user story 166
 /*  06/18/2019    Ranadheer Reddy                1.2      Updated for user story 167, 168 and 169
 /******************************************************************************/
 
   /* *************************************************************************************
   Name: process_operations_main
   Object Type: Procedure
   Description: Main Procedure to perform operations conversion
   Input params: p_tech_id
   p_tech_type
   p_process_name
   p_start_date

   Out params:   p_x_errbuf
                 p_x_retcode


   Change History:
   Date         Name                      Ver    Modification
   -----------  ----------------------    ----   ------------
   05/13/2019    Trey Hernandez           1.0    Initial Creation
   06/18/2019    Yu Jian Lao              1.1    Updated for user story 166
   06/18/2019    Ranadheer Reddy          1.2    Updated for user story 167, 168 and 169
   ************************************************************************************* */
   PROCEDURE process_operations_main(
          p_x_errbuf              OUT VARCHAR2
         ,p_x_retcode             OUT NUMBER
         ,p_conv_mode              IN VARCHAR2) IS

      l_dbg_msg VARCHAR2(2000);
      l_tech_id NUMBER;
      l_instance_id NUMBER;
      l_user_id NUMBER := MROI_UTIL_PKG.get_user_id('CONVERSION');
      l_org_id NUMBER :=  MROI_UTIL_PKG.get_master_org_id; -- master org ID for master items, this param might need to change if importing items for othe Orgs
      l_process_id NUMBER;
      l_start_date DATE;

      l_total_trx_count NUMBER := 0;
      l_error_count NUMBER := 0;
      l_files_count NUMBER := 0;
      l_x_mimes_err_code NUMBER;
      l_x_mimes_err_msg VARCHAR2(2000);
      l_x_mroi_err_code NUMBER;
      l_x_mroi_err_msg VARCHAR2(2000);
      l_inst_status_code NUMBER;
      p_tech_type VARCHAR2(20) := 'CONC_REQ';
      p_process_name VARCHAR2(20) := 'MROI_OPERATION_CNV';
    l_source_file_name VARCHAR2(100) := '*.csv';
    l_source_directory VARCHAR2(100) := 'MROI_OP_CNV';
    l_source_archive_directory VARCHAR2(100) := 'MROI_OP_CNV_ARCHIVE';
    l_source_file_ext_tbl VARCHAR2(100) := 'GET_AHL_OPS_CNV_FILES';
    l_script_name VARCHAR2(100) := 'dir_list_ahl_ops.sh';
    l_file_names VARCHAR2(1000) := '';
    l_archive_file_names VARCHAR2(1000) := '';
    l_archive_file_name_list VARCHAR2(4000);
    l_t_ops_hdr_stg t_ops_hdr_stg_tbl_type;
    l_failed_api_count  NUMBER;
    l_ahl_ops_count   NUMBER;

    e_no_records_exception EXCEPTION;
    e_no_files_exception EXCEPTION;
    e_mimes_exception EXCEPTION;

BEGIN
      --todo uncomment in ZONE B VDI dev instance
     
      DBMS_OUTPUT.PUT_LINE('*******************************************************');
      DBMS_OUTPUT.PUT_LINE('Started: ' || TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
      DBMS_OUTPUT.PUT_LINE('*******************************************************');


      l_tech_id := fnd_global.conc_request_id;
      l_start_date  := SYSDATE;


    l_dbg_msg :='100.00 Calling MROI_UTIL_PKG.get_file_names to get Conversion Source File name(s). ';
    BEGIN
    mroi_util_pkg.get_file_names(              p_file_name                  => l_source_file_name
                                              ,p_file_directory             => l_source_directory
                                              ,p_external_table_name        => l_source_file_ext_tbl
                                              ,p_script_name                => l_script_name
                                              ,p_script_directory           => l_source_archive_directory
                                              ,p_file_count                 => l_files_count
                                              ,p_file_name_list             => l_file_names
                                              ,p_error_msg                  => p_x_errbuf);

    IF l_files_count = 0 or l_files_count IS NULL THEN
     RAISE e_no_files_exception;
    END IF;
    END;
FND_FILE.PUT_LINE(FND_FILE.log,l_file_names); 
    l_dbg_msg := '100.02 Calling MROI_TT_STATUS_UTIL_PKG.initialize_instance. ';
    MROI_TT_STATUS_UTIL_PKG.initialize_instance(p_tech_id        => l_tech_id
                                               ,p_tech_type      => p_tech_type -- possible values are SOA_COMPOSITE, SOA_BPEL, CONC_REQ, ODI_LOAD_PLAN
                                               ,p_process_id     => MROI_TT_STATUS_UTIL_PKG.get_process_id(p_process_name)--l_process_id
                                               ,p_file_name      => l_file_names
                                               ,p_start_date     => l_start_date
                                               ,p_ec_flag        => 'N' -- values Y or N --indicates if instance has been invoked by reprocessing module (in case reprocessing functionality being used)
                                               ,p_user_id        => l_user_id
                                               ,p_attribute1     => NULL
                                               ,p_attribute2     => NULL
                                               ,p_attribute3     => NULL
                                               ,p_attribute4     => NULL
                                               ,p_attribute5     => NULL
                                               ,p_x_instance_id  => l_instance_id
                                               ,p_x_mimes_err_code => l_x_mimes_err_code
                                               ,p_x_mimes_err_msg  => l_x_mimes_err_msg);

     IF l_x_mimes_err_code = 1 THEN
        RAISE e_mimes_exception;
     END IF;

     l_dbg_msg := '100.03 Calling mroi_ahl_ops_conv_pkg.stage_ahl_operations_records. ';
     stage_ahl_operations_records( p_instance_id         => l_instance_id
                                  ,p_user_id             => l_user_id
                                  ,p_file_name           => l_source_file_name
                                  ,p_tech_id             => l_tech_id
                                  ,p_x_total_trx_count   => l_total_trx_count
                                  ,p_x_mroi_err_code     => p_x_retcode
                                  ,p_x_mroi_err_msg      => p_x_errbuf
                                  ,p_x_mimes_err_code    => l_x_mimes_err_code
                                  ,p_x_mimes_err_msg     => l_x_mimes_err_msg);

     IF l_x_mimes_err_code = 1 THEN
        RAISE e_mimes_exception;
     END IF;

     IF p_x_retcode = 2 THEN
        RAISE e_no_records_exception;
     END IF;

     l_dbg_msg := 'moving file: ' || l_source_file_name ||' to archive directory';
      --Drop the external table which reference list of files based on the file name pattern
      mroi_util_pkg.archive_files(p_external_table_name     => l_source_file_ext_tbl, 
                                  p_script_name             => l_script_name, 
                                  p_file_directory          => l_source_directory, 
                                  p_file_archive_directory  => l_source_archive_directory, 
                                  p_script_directory        => l_source_archive_directory, 
                                  p_archive_file_name_list  => l_archive_file_names, 
                                  p_error_msg               => p_x_errbuf );


      IF p_x_errbuf IS NOT NULL THEN
      dbms_output.put_line('Error returned from mroi_util_pkg.archive_files: ' || p_x_errbuf);
      END IF;

     l_dbg_msg := '100.05 Calling MROI_TT_STATUS_UTIL_PKG.update_instance_status. ';
     MROI_TT_STATUS_UTIL_PKG.update_instance_status(p_instance_id        => l_instance_id
                                                  ,p_status_code         => MROI_TT_STATUS_UTIL_PKG.G_INSTCODE_RECS_STAGED
                                                  ,p_archive_file_name   => l_archive_file_names
                                                  ,p_received_file_count => l_files_count
                                                  ,p_header_trx_count    => ''
                                                  ,p_received_trx_count  => l_total_trx_count
                                                  ,p_header_dollar_amt   => ''
                                                  ,p_received_dollar_amt => ''
                                                  ,p_sent_trx_count      => ''
                                                  ,p_files_sequence      => ''
                                                  ,p_user_id             => l_user_id
                                                  ,p_attribute1          => ''
                                                  ,p_attribute2          => ''
                                                  ,p_attribute3          => ''
                                                  ,p_attribute4          => ''
                                                  ,p_attribute5          => ''
                                                  ,p_x_mimes_err_code    => l_x_mimes_err_code
                                                  ,p_x_mimes_err_msg     => l_x_mimes_err_msg);

     IF l_x_mimes_err_code = 1 THEN
        RAISE e_mimes_exception;
     END IF;

     l_dbg_msg := '100.06 Calling mroi_ahl_ops_conv_pkg.validate_operations_records. ';
     validate_operations_records( l_instance_id
                                 ,l_user_id
                                 ,l_error_count
                                 ,l_t_ops_hdr_stg
                                 ,l_x_mroi_err_code
                                 ,l_x_mroi_err_msg
                                 ,l_x_mimes_err_code
                                 ,l_x_mimes_err_msg);

     IF l_x_mimes_err_code = 1 THEN
        RAISE e_mimes_exception;
     END IF;
     
     l_dbg_msg := '100.07 Updating instance status after validation. ';
     MROI_TT_STATUS_UTIL_PKG.update_instance_status(p_instance_id        => l_instance_id
                                                  ,p_status_code         => MROI_TT_STATUS_UTIL_PKG.G_INSTCODE_RECS_VALIDATED
                                                  ,p_archive_file_name   => ''
                                                  ,p_received_file_count => ''
                                                  ,p_header_trx_count    => ''
                                                  ,p_received_trx_count  => ''
                                                  ,p_header_dollar_amt   => ''
                                                  ,p_received_dollar_amt => ''
                                                  ,p_sent_trx_count      => ''
                                                  ,p_files_sequence      => ''
                                                  ,p_user_id             => l_user_id
                                                  ,p_attribute1          => ''
                                                  ,p_attribute2          => ''
                                                  ,p_attribute3          => ''
                                                  ,p_attribute4          => ''
                                                  ,p_attribute5          => ''
                                                  ,p_x_mimes_err_code    => l_x_mimes_err_code
                                                  ,p_x_mimes_err_msg     => l_x_mimes_err_msg);

     IF l_x_mimes_err_code = 1 THEN
        RAISE e_mimes_exception;
     END IF;

     l_dbg_msg := '100.08 Calling mroi_ahl_ops_conv_pkg.create_operations_records. ';
     create_operations_records( l_instance_id
                               ,l_user_id
                               ,p_conv_mode
                               ,l_t_ops_hdr_stg
                               ,l_failed_api_count
                               ,l_ahl_ops_count
                               ,l_x_mroi_err_code
                               ,l_x_mroi_err_msg
                               ,l_x_mimes_err_code
                               ,l_x_mimes_err_msg);

     IF l_x_mimes_err_code = 1 THEN
        RAISE e_mimes_exception;
     END IF;

     l_dbg_msg := '100.09 Updating instance status after validation. ';
     MROI_TT_STATUS_UTIL_PKG.update_instance_status(p_instance_id        => l_instance_id
                                                  ,p_status_code         => MROI_TT_STATUS_UTIL_PKG.G_INSTCODE_RECS_LOADED_OIT_API
                                                  ,p_archive_file_name   => ''
                                                  ,p_received_file_count => ''
                                                  ,p_header_trx_count    => ''
                                                  ,p_received_trx_count  => ''
                                                  ,p_header_dollar_amt   => ''
                                                  ,p_received_dollar_amt => ''
                                                  ,p_sent_trx_count      => ''
                                                  ,p_files_sequence      => ''
                                                  ,p_user_id             => l_user_id
                                                  ,p_attribute1          => ''
                                                  ,p_attribute2          => ''
                                                  ,p_attribute3          => ''
                                                  ,p_attribute4          => ''
                                                  ,p_attribute5          => ''
                                                  ,p_x_mimes_err_code    => l_x_mimes_err_code
                                                  ,p_x_mimes_err_msg     => l_x_mimes_err_msg);

     IF l_x_mimes_err_code = 1 THEN
        RAISE e_mimes_exception;
     END IF;
     
     l_dbg_msg := '100.09 Updating instance status after validation. ';
     MROI_TT_STATUS_UTIL_PKG.update_instance_status(p_instance_id        => l_instance_id
                                                  ,p_status_code         => MROI_TT_STATUS_UTIL_PKG.G_INSTCODE_COMPLETED
                                                  ,p_archive_file_name   => ''
                                                  ,p_received_file_count => ''
                                                  ,p_header_trx_count    => ''
                                                  ,p_received_trx_count  => ''
                                                  ,p_header_dollar_amt   => ''
                                                  ,p_received_dollar_amt => ''
                                                  ,p_sent_trx_count      => ''
                                                  ,p_files_sequence      => ''
                                                  ,p_user_id             => l_user_id
                                                  ,p_attribute1          => ''
                                                  ,p_attribute2          => ''
                                                  ,p_attribute3          => ''
                                                  ,p_attribute4          => ''
                                                  ,p_attribute5          => ''
                                                  ,p_x_mimes_err_code    => l_x_mimes_err_code
                                                  ,p_x_mimes_err_msg     => l_x_mimes_err_msg);

     IF l_x_mimes_err_code = 1 THEN
        RAISE e_mimes_exception;
     END IF;
     
     conversion_report (p_start_date =>  l_start_date, 
                        p_end_date => sysdate, 
                        p_file_name => l_file_names, 
                        p_total_record => l_total_trx_count, 
                        p_total_success_record => l_t_ops_hdr_stg.count, 
                        p_total_failed_val_record => l_total_trx_count - l_t_ops_hdr_stg.count, 
                        p_total_failed_api_record => l_failed_api_count, 
                        p_total_ops_record => l_ahl_ops_count, 
                        p_conv_mode => p_conv_mode);
     write_transaction_status(l_instance_id
                                    ,l_x_mroi_err_code
                                    ,l_x_mroi_err_msg);
--
--      FND_FILE.PUT_LINE(FND_FILE.OUTPUT,'*******************************************************');
--      FND_FILE.PUT_LINE(FND_FILE.OUTPUT,'Finished: ' || TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
--      FND_FILE.PUT_LINE(FND_FILE.OUTPUT,'*******************************************************'); 

   EXCEPTION
      WHEN e_mimes_exception THEN
         p_x_retcode := l_x_mimes_err_code;
         p_x_errbuf := 'l_x_mimes_err_msg '||l_x_mimes_err_msg;
         dbms_output.put_line('e_mimes_exception');
         FND_FILE.PUT_LINE(FND_FILE.LOG, 'MIMES error in procedure MROI_AHL_OPS_CONV_PKG.process_operations_main: ' || l_x_mimes_err_msg);
         -- todo: will be setting instance status code to G_INSTCODE_ERROR here if MIMES API returned an error
                 MROI_TT_STATUS_UTIL_PKG.update_instance_status(p_instance_id        => l_instance_id
                                                  ,p_status_code         => MROI_TT_STATUS_UTIL_PKG.G_INSTCODE_ERROR
                                                  ,p_archive_file_name   => ''
                                                  ,p_received_file_count => ''
                                                  ,p_header_trx_count    => ''
                                                  ,p_received_trx_count  => ''
                                                  ,p_header_dollar_amt   => ''
                                                  ,p_received_dollar_amt => ''
                                                  ,p_sent_trx_count      => ''
                                                  ,p_files_sequence      => ''
                                                  ,p_user_id             => l_user_id
                                                  ,p_attribute1          => ''
                                                  ,p_attribute2          => ''
                                                  ,p_attribute3          => ''
                                                  ,p_attribute4          => ''
                                                  ,p_attribute5          => ''
                                                  ,p_x_mimes_err_code    => l_x_mimes_err_code
                                                  ,p_x_mimes_err_msg     => l_x_mimes_err_msg);
    WHEN e_no_records_exception THEN
        dbms_output.put_line('No Records are available to load. Aborting current run.  Please check the associated timestamped badfile and logfile for further details.');
        FND_FILE.PUT_LINE(FND_FILE.LOG, 'No Records are available to load. Aborting current run.  Please check the associated timestamped badfile and logfile for further details.');
        MROI_TT_STATUS_UTIL_PKG.update_instance_status(p_instance_id        => l_instance_id
                                                  ,p_status_code         => MROI_TT_STATUS_UTIL_PKG.G_INSTCODE_ERROR
                                                  ,p_archive_file_name   => ''
                                                  ,p_received_file_count => ''
                                                  ,p_header_trx_count    => ''
                                                  ,p_received_trx_count  => ''
                                                  ,p_header_dollar_amt   => ''
                                                  ,p_received_dollar_amt => ''
                                                  ,p_sent_trx_count      => ''
                                                  ,p_files_sequence      => ''
                                                  ,p_user_id             => l_user_id
                                                  ,p_attribute1          => ''
                                                  ,p_attribute2          => ''
                                                  ,p_attribute3          => ''
                                                  ,p_attribute4          => ''
                                                  ,p_attribute5          => ''
                                                  ,p_x_mimes_err_code    => l_x_mimes_err_code
                                                  ,p_x_mimes_err_msg     => l_x_mimes_err_msg);
                                                  
    WHEN e_no_files_exception THEN
        FND_FILE.PUT_LINE(FND_FILE.OUTPUT, 'No files to read. Aborting current run.  Please check that the file is in the proper directory.');
    WHEN OTHERS THEN
        p_x_errbuf := 'Unexpected error occured in MROI_AHL_OPS_CONV_PKG.process_operations_main when '||l_dbg_msg||' '||SQLERRM;
        p_x_retcode := 1;
        FND_FILE.PUT_LINE(FND_FILE.LOG, p_x_errbuf);
        dbms_output.put_line(substr('Unexpected exception occurred in procedure PROCESS_OPERATIONS_MAIN: ' || sqlerrm, 1, 2000));
        dbms_output.put_line('l_dbg_msg: ' ||l_dbg_msg);
   END process_operations_main;

PROCEDURE conversion_report (p_start_date IN Date, p_end_date IN date, p_file_name IN VARCHAR2, p_total_record IN NUMBER, p_total_success_record IN NUMBER, p_total_failed_val_record IN NUMBER, p_total_failed_api_record IN NUMBER, p_total_ops_record IN NUMBER, p_conv_mode IN VARCHAR2) IS
Begin 
      
      FND_FILE.PUT_LINE(FND_FILE.OUTPUT,'-----------------------RICEW ID: 0006 - MROI Operations Conversion status Report-----------------------------------');
      FND_FILE.PUT_LINE(FND_FILE.OUTPUT,'');
      FND_FILE.PUT_LINE(FND_FILE.OUTPUT,'Start time: ' || to_char(p_start_date, 'DD-MON-YYYY HH24:MI:SS'));
      FND_FILE.PUT_LINE(FND_FILE.OUTPUT,'  End time: ' || to_char(p_end_date, 'DD-MON-YYYY HH24:MI:SS'));
      FND_FILE.PUT_LINE(FND_FILE.OUTPUT,'');
      FND_FILE.PUT_LINE(FND_FILE.OUTPUT,'Name of the file dropped: ' || p_file_name);
      FND_FILE.PUT_LINE(FND_FILE.OUTPUT,'');
      FND_FILE.PUT_LINE(FND_FILE.OUTPUT,'          Execution mode: ' || (CASE when p_conv_mode = 'L' THEN 'Load' ELSE 'Validate' END));
      FND_FILE.PUT_LINE(FND_FILE.OUTPUT,'');
      FND_FILE.PUT_LINE(FND_FILE.OUTPUT,'  Total Records selected for processing: ' || p_total_record);
      FND_FILE.PUT_LINE(FND_FILE.OUTPUT,'   Total Records successfully validated: ' || p_total_success_record);
      FND_FILE.PUT_LINE(FND_FILE.OUTPUT,'Total Records failed initial validation: ' || p_total_failed_val_record);
      FND_FILE.PUT_LINE(FND_FILE.OUTPUT,'       Total Records failed API process: ' || p_total_failed_api_record);
      IF p_conv_mode = 'L' THEN 
            FND_FILE.PUT_LINE(FND_FILE.OUTPUT,'     Total Number of Operations Created: ' || p_total_ops_record);
      END IF;
End;      

/* *************************************************************************************
 Name: stage_ahl_operations_records
 Object Type: Procedure
 Description:
 Input params: p_file_name
               p_user_id
               p_file_name

 Out params:   p_x_total_trx_count
          p_x_mroi_err_code
          p_x_mroi_err_msg
          p_x_mimes_err_code
          p_x_mimes_err_msg

 Change History:
 Date         Name                      Ver    Modification
 -----------  ----------------------    ----   ------------
 04/30/2019    Trey Hernandez        1.0     Initial Creation
 ************************************************************************************* */

      PROCEDURE stage_ahl_operations_records(  p_instance_id IN NUMBER
                                                            ,p_user_id IN NUMBER
                                                                  ,p_file_name IN VARCHAR2
                                                                  ,p_tech_id IN NUMBER
                                                                  ,p_x_total_trx_count OUT NUMBER
                                                                  ,p_x_mroi_err_code OUT NUMBER
                                                                  ,p_x_mroi_err_msg OUT VARCHAR2
                                                                  ,p_x_mimes_err_code OUT NUMBER
                                                                  ,p_x_mimes_err_msg OUT VARCHAR2)
    IS

     CURSOR c_ops_ext_table IS
      SELECT *
       FROM xxmro.mroi_ahl_ops_ext
      WHERE OPERATION_ID IS NOT NULL OR DESCRIPTION IS NOT NULL OR OPERATION_TYPE IS NOT NULL OR 
            START_DATE IS NOT NULL OR QUALITY_INSPECTION_TYPE IS NOT NULL OR STANDARD IS NOT NULL OR
            RR_SCHEDULE_SEQUENCE IS NOT NULL OR RR_RESOURCE_TYPE IS NOT NULL OR RR_RESOURCE IS NOT NULL OR 
            RR_QUANTITY IS NOT NULL OR RR_DURATION IS NOT NULL OR MR_ITEMS IS NOT NULL OR
            MR_DESCRIPTION IS NOT NULL OR MR_QUANTITY IS NOT NULL OR RD_DOCUMENT_NUM IS NOT NULL OR
            SECONDARY_BUYOFF IS NOT NULL OR COMPETENCY_NAME IS NOT NULL OR OCCURRENCE_FACTOR IS NOT NULL            
      ;


  l_record_header_id    NUMBER;
  l_sql                 VARCHAR2(4000);
  l_header_record_count NUMBER := 0;
  l_line_record_count   NUMBER := 0;
  l_xtern_count         NUMBER :=0;
  l_hdr_insert_count    NUMBER := 0;
  l_line_insert_count   NUMBER := 0;
  l_file_count          NUMBER := 0;
  l_bad_file_exist      BOOLEAN;
  l_directory           VARCHAR2(4000);
  l_dbg_msg             VARCHAR2(200);
  l_x_mimes_err_code    NUMBER;
  l_x_mimes_err_msg     VARCHAR2(1000);
  l_trx_id              NUMBER;
  e_mimes_exception     EXCEPTION;
  l_file_exist          NUMBER;

  l_current_date_time   VARCHAR2(20) := to_char(sysdate, 'DD-MON-YYYY_HH24:MI:SS');

  --define table type variables for collections
  l_t_external_table t_external_table_type;  --table type variable for external table
  l_t_ops_hdr_stg t_ops_hdr_stg_tbl_type := t_ops_hdr_stg_tbl_type (); --table type variable for hdr stg table
  l_t_ops_ln_stg t_ops_ln_stg_tbl_type := t_ops_ln_stg_tbl_type();--table type variable for line stg table
  l_t_trx_ids_tbl  MROI_TT_STATUS_UTIL_PKG.t_trx_id_tbl := MROI_TT_STATUS_UTIL_PKG.t_trx_id_tbl();
BEGIN

      p_x_mroi_err_code := 0;
      p_x_mroi_err_msg := '';
      p_x_total_trx_count := 0;

      l_dbg_msg :='. Inserted record into MROI_TT_INSTANCE_STATUS table.';

      --2. Use EXTERNAL TABLE DDL within PL/SQL to create an external table referencing conversion datafile.
      l_dbg_msg :='. Dynamically altering external table';
   EXECUTE IMMEDIATE ('ALTER TABLE xxmro.mroi_ahl_ops_ext
                        ACCESS PARAMETERS
                       ( RECORDS DELIMITED BY NEWLINE
                         BADFILE MROI_OP_CNV_ARCHIVE:''mroi_ahl_ops_ext_' || l_current_date_time || '.bad''' ||
                         'LOGFILE MROI_OP_CNV_ARCHIVE:''mroi_ahl_ops_ext_' || l_current_date_time || '.log''' ||
                         'SKIP 0
                         FIELDS CSV WITH EMBEDDED
                         LRTRIM
                         MISSING FIELD VALUES ARE NULL
                         ) LOCATION (''' || p_file_name || ''')
                           REJECT LIMIT UNLIMITED');

  --verify directory path exists for directory
  l_dbg_msg :='. Getting source file directory path';

   SELECT DIRECTORY_PATH
    INTO l_directory
   FROM all_directories
   WHERE directory_name =  'MROI_OP_CNV';

  --check bad file exists or not when external table is created
  l_dbg_msg :='. Query external table data.';
   SELECT 1
    INTO l_file_exist
   FROM mroi_ahl_ops_ext
   WHERE ROWNUM =1;

  l_dbg_msg :='. Getting total counts of external table';
  --Gathering total count for both header and lines

    SELECT SUM(CASE WHEN OPERATION_ID is not null THEN 2 ELSE 1  END)
     INTO l_xtern_count
    FROM xxmro.mroi_ahl_ops_ext
    WHERE  OPERATION_ID IS NOT NULL OR DESCRIPTION IS NOT NULL OR OPERATION_TYPE IS NOT NULL OR 
            START_DATE IS NOT NULL OR QUALITY_INSPECTION_TYPE IS NOT NULL OR STANDARD IS NOT NULL OR
            RR_SCHEDULE_SEQUENCE IS NOT NULL OR RR_RESOURCE_TYPE IS NOT NULL OR RR_RESOURCE IS NOT NULL OR 
            RR_QUANTITY IS NOT NULL OR RR_DURATION IS NOT NULL OR MR_ITEMS IS NOT NULL OR
            MR_DESCRIPTION IS NOT NULL OR MR_QUANTITY IS NOT NULL OR RD_DOCUMENT_NUM IS NOT NULL OR
            SECONDARY_BUYOFF IS NOT NULL OR COMPETENCY_NAME IS NOT NULL OR OCCURRENCE_FACTOR IS NOT NULL;


  --Open cursor to query all recently created records in the external table mroi_ahl_ops_ext
  l_dbg_msg :='. Opening cursor for external table records';

  IF c_ops_ext_table%ISOPEN THEN
   CLOSE c_ops_ext_table;
  END IF;

   OPEN c_ops_ext_table;
    LOOP
     --load records into the external table collection variable
     l_dbg_msg :='.  Bulk Collecting external table data into collection';

     FETCH c_ops_ext_table BULK COLLECT INTO l_t_external_table LIMIT 10000;

     --for each record in the collection variable, assign values to the header/line staging table collection

     FOR i IN 1 .. l_t_external_table.COUNT
      LOOP

       IF TRIM(l_t_external_table(i).operation_id) IS NOT NULL THEN

        l_dbg_msg :='. Assigning each external record to header staging record.';

          l_record_header_id := XXMRO.MROI_AHL_OPS_HDR_STG_S.NEXTVAL;
          l_trx_id := MROI_TT_TRX_STATUS_S.NEXTVAL;

          l_t_ops_hdr_stg.EXTEND;
          l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).record_header_id := l_record_header_id;
          l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).transaction_id := l_trx_id;
          l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).instance_id := p_instance_id;
          l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).conc_request_id := p_tech_id;
          l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).operation_id := l_t_external_table(i).operation_id;
          l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).description := l_t_external_table(i).description;
          l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).operation_type := l_t_external_table(i).operation_type;
          l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).start_date := to_date(l_t_external_table(i).start_date, 'MM/DD/YYYY');
          l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).quality_inspection_type := l_t_external_table(i).quality_inspection_type;
          l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).standard := l_t_external_table(i).standard;
          l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).competency_name := l_t_external_table(i).competency_name ;
          l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).occurrence_factor := l_t_external_table(i).occurrence_factor ;
          l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).secondary_buyoff := l_t_external_table(i).secondary_buyoff;
          l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).creation_date := SYSDATE;
          l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).created_by := p_user_id;
          l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).last_update_date := SYSDATE;
          l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).last_updated_by := p_user_id;
          l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).status := 'NEW';
          l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).error_message := NULL;

          --populate the transaction id collection
          l_t_trx_ids_tbl.EXTEND;
          l_t_trx_ids_tbl(l_t_trx_ids_tbl.COUNT).TRANSACTION_ID := l_trx_id;

       END IF;

         l_dbg_msg :='. Assigning each external record to header staging record.';

          l_t_ops_ln_stg.EXTEND;
          l_t_ops_ln_stg(l_t_ops_ln_stg.count).record_line_id := xxmro.MROI_AHL_OPS_LINE_STG_S.NEXTVAL;
          l_t_ops_ln_stg(l_t_ops_ln_stg.count).record_header_id := l_record_header_id;
          l_t_ops_ln_stg(l_t_ops_ln_stg.count).rr_schedule_sequence := l_t_external_table(i).rr_schedule_sequence;
          l_t_ops_ln_stg(l_t_ops_ln_stg.count).rr_resource_type := l_t_external_table(i).rr_resource_type;
          l_t_ops_ln_stg(l_t_ops_ln_stg.count).rr_resource_id := NULL;
          l_t_ops_ln_stg(l_t_ops_ln_stg.count).rr_resource := l_t_external_table(i).rr_resource;
          l_t_ops_ln_stg(l_t_ops_ln_stg.count).rr_quantity := l_t_external_table(i).rr_quantity;
          l_t_ops_ln_stg(l_t_ops_ln_stg.count).rr_duration := l_t_external_table(i).rr_duration;
          l_t_ops_ln_stg(l_t_ops_ln_stg.count).mr_item_id := NULL;
          l_t_ops_ln_stg(l_t_ops_ln_stg.count).mr_item := l_t_external_table(i).mr_items;
          l_t_ops_ln_stg(l_t_ops_ln_stg.count).mr_description := l_t_external_table(i).mr_description;
          l_t_ops_ln_stg(l_t_ops_ln_stg.count).mr_quantity := l_t_external_table(i).mr_quantity;
          l_t_ops_ln_stg(l_t_ops_ln_stg.count).mr_item_org_code := NULL;
          l_t_ops_ln_stg(l_t_ops_ln_stg.count).mr_item_org_id := NULL;
          l_t_ops_ln_stg(l_t_ops_ln_stg.count).rd_document_id := NULL;
          l_t_ops_ln_stg(l_t_ops_ln_stg.count).rd_document_num := l_t_external_table(i).rd_document_num;
          l_t_ops_ln_stg(l_t_ops_ln_stg.count).creation_date := SYSDATE;
          l_t_ops_ln_stg(l_t_ops_ln_stg.count).created_by := p_user_id;
          l_t_ops_ln_stg(l_t_ops_ln_stg.count).last_update_date := SYSDATE;
          l_t_ops_ln_stg(l_t_ops_ln_stg.count).last_updated_by := p_user_id;

      END LOOP;

      --perform mass insert for each 10000 records batch
      l_dbg_msg :='. Inserting each batch into Header Staging Table';
      FORALL i IN l_t_ops_hdr_stg.FIRST..l_t_ops_hdr_stg.LAST
       INSERT INTO MROI_AHL_OPS_HDR_STG VALUES l_t_ops_hdr_stg(i);

       l_hdr_insert_count:= SQL%ROWCOUNT + l_hdr_insert_count;
       p_x_total_trx_count:= SQL%ROWCOUNT + p_x_total_trx_count;

      l_dbg_msg :='. Inserting each batch into Lines Staging Table';
       FORALL i IN l_t_ops_ln_stg.FIRST..l_t_ops_ln_stg.LAST
        INSERT INTO MROI_AHL_OPS_LINE_STG VALUES l_t_ops_ln_stg(i);

        l_line_insert_count := SQL%ROWCOUNT + l_line_insert_count;
        --p_x_total_trx_count:= SQL%ROWCOUNT + p_x_total_trx_count;

        -- call MIMES API to insert generated collection of transaction ids into MROI_TT_TRX_STATUS table
            l_dbg_msg := '009 Calling MROI_TT_STATUS_UTIL_PKG.initialize_trx_collection. ';

            MROI_TT_STATUS_UTIL_PKG.initialize_trx_collection(p_instance_id      => p_instance_id
                                                             ,p_user_id          => p_user_id
                                                             ,p_trx_ids_tbl      => l_t_trx_ids_tbl
                                                             ,p_x_mimes_err_code => l_x_mimes_err_code
                                                             ,p_x_mimes_err_msg  => l_x_mimes_err_msg);


            IF l_x_mimes_err_code = 1 THEN
                RAISE e_mimes_exception;
            END IF;

        --need to empy collections for next batch
       l_t_ops_hdr_stg.DELETE;
       l_t_ops_ln_stg.DELETE;
       l_t_trx_ids_tbl.DELETE;

       EXIT WHEN c_ops_ext_table%NOTFOUND;

       END LOOP;

       COMMIT;

        IF mroi_util_pkg.file_exists('mroi_ahl_ops_ext_' || l_current_date_time || '.bad', 'MROI_OP_CNV_ARCHIVE') THEN
          dbms_output.put_line('Bad file exists. Bad file name: ' || 'mroi_ahl_ops_ext_' || l_current_date_time || '.bad');
          dbms_output.put_line('Please check your associated, timestamped badfile and log file for further information. ');
        END IF;

       --close cursor
       l_dbg_msg :='. Closing c_ops_ext_table cursor. ';
       IF c_ops_ext_table%ISOPEN THEN
        CLOSE c_ops_ext_table;
       END IF;

--------------------------------------------------------------------------------
--       fnd_file.put_line(FND_FILE.OUTPUT,rpad('Instance ID: ',60)||p_instance_id);
--       fnd_file.put_line(FND_FILE.OUTPUT,rpad('Total Records to Stage: ',60)||l_xtern_count);       
--       fnd_file.put_line(FND_FILE.OUTPUT,rpad('Total Header Records Staged: ', 60) || l_hdr_insert_count);
--       fnd_file.put_line(FND_FILE.OUTPUT,rpad('Total Lines Records Staged: ', 60) || l_line_insert_count);
--       fnd_file.put_line(FND_FILE.OUTPUT,rpad('Total Records Staged: ',60)|| p_x_total_trx_count);
       
       dbms_output.put_line(rpad('Total Records to Stage: ',60)||l_xtern_count);
       dbms_output.put_line(rpad('Total Header Records Staged: ', 60) || l_hdr_insert_count);
       dbms_output.put_line(rpad('Total Lines Records Staged: ', 60) || l_line_insert_count);
       dbms_output.put_line(rpad('Total Records Staged: ',60)|| p_x_total_trx_count);
EXCEPTION
 WHEN e_mimes_exception THEN
         DBMS_OUTPUT.PUT_LINE ('e_mimes_exception');
         p_x_mimes_err_code := l_x_mimes_err_code;
         p_x_mimes_err_msg := l_x_mimes_err_msg;
         FND_FILE.PUT_LINE(FND_FILE.LOG, 'MIMES error in procedure MROI_AHL_OPS_CONV_PKG.stage_ahl_operations_records: ' || l_x_mimes_err_msg);
 WHEN NO_DATA_FOUND THEN
         p_x_mroi_err_code := 2;
         p_x_mroi_err_msg:= 'No Data Found in the the External Table';
         FND_FILE.PUT_LINE(FND_FILE.LOG, p_x_mroi_err_msg);
 WHEN OTHERS THEN
         p_x_mroi_err_code := 3;
         p_x_mroi_err_msg := substr('Unexpected exception occurred in procedure STAGE_FILE_RECORDS: ' || sqlerrm, 1, 2000);
         DBMS_OUTPUT.PUT_LINE('Unexpected exception occurred at ' || l_dbg_msg || ' in procedure STAGE_FILE_RECORDS: ' || sqlerrm);
         FND_FILE.PUT_LINE(FND_FILE.LOG, 'Unexpected exception occurred at ' || l_dbg_msg || ' in procedure STAGE_FILE_RECORDS: ' || sqlerrm);
END stage_ahl_operations_records;

 /* *************************************************************************************
   Name: is_numeric 
   Object Type: Procedure
   Description:
   Input params: p_instance_id
                 p_x_mroi_err_code
                 p_x_mroi_err_msg
   Out params:
   -----------  ----------------------    ----   ------------
   06/10/2019   Yu Jian Lao           1.0    Initial Creation
   ************************************************************************************* */

FUNCTION is_numeric (p_value IN VARCHAR2) RETURN NUMBER AS
   l_digit NUMBER;
BEGIN
   l_digit := p_value;
   RETURN 1;
EXCEPTiON
   WHEN OTHERS THEN 
        RETURN 0;
END is_numeric;

 /* *************************************************************************************
   Name: output_errors
   Object Type: Procedure
   Description:
   Input params: p_instance_id
                 p_x_mroi_err_code
                 p_x_mroi_err_msg
   Out params:
   -----------  ----------------------    ----   ------------
   06/10/2019   Yu Jian Lao           1.0    Initial Creation
   ************************************************************************************* */ 
PROCEDURE output_errors(p_instance_id IN VARCHAR2                       
                       ,p_x_mroi_err_code OUT NUMBER
                       ,p_x_mroi_err_msg OUT VARCHAR2) IS
                       
    CURSOR c_hdr_recs(v_instance_id IN NUMBER) IS
        SELECT stg.RECORD_HEADER_ID, stg.TRANSACTION_ID, stg.OPERATION_ID   
              ,stg.DESCRIPTION, stg.OPERATION_TYPE 
              ,stg.start_date, stg.quality_inspection_type, stg.standard 
              ,stg.competency_name ,stg.occurrence_factor, stg.status, stg.secondary_buyoff             
        FROM MROI_AHL_OPS_HDR_STG stg 
        WHERE stg.INSTANCE_ID = v_instance_id
        order by record_header_id 
        ;                   

    CURSOR c_hdr_erred_recs(v_record_header_id IN NUMBER) IS
        SELECT err.ERROR_CODE, err.ERROR_FIELD, err.ERROR_ATTRIBUTE1 
              ,err.ERROR_ATTRIBUTE2, err.ERROR_ATTRIBUTE3, err.ERROR_ATTRIBUTE4
              ,err.ORA_ERR_CODE,err.ORA_ERR_MSG
        FROM MROI_AHL_OPS_ERR err,
             MROI_AHL_OPS_HDR_STG stg 
        WHERE stg.record_header_id = err.RECORD_LEVEL_ID 
          AND stg.record_header_id = v_record_header_id
          AND err.RECORD_LEVEL_TYPE = 'HEADER'
          AND err.err_old_flag = 'N'
        ;

    CURSOR c_line_erred_recs(v_record_header_id IN NUMBER) IS 
        SELECT stg.record_header_id, stg.record_line_id
              ,stg.rr_schedule_sequence  
              ,stg.rr_resource_type, stg.rr_resource 
              ,stg.rr_quantity, stg.rr_duration 
              ,stg.mr_item
              ,stg.mr_description, stg.mr_quantity
              ,stg.rd_document_num
              ,err.ERROR_CODE, err.ERROR_FIELD, err.ERROR_ATTRIBUTE1
              ,err.ERROR_ATTRIBUTE2, err.ERROR_ATTRIBUTE3, err.ERROR_ATTRIBUTE4
              ,err.ORA_ERR_CODE,err.ORA_ERR_MSG
        FROM MROI_AHL_OPS_ERR err,
             MROI_AHL_OPS_LINE_STG stg 
        WHERE err.RECORD_LEVEL_ID = stg.RECORD_LINE_ID
        AND  v_record_header_id = stg.record_header_id
        AND  err.RECORD_LEVEL_TYPE = 'LINE'
        AND err.err_old_flag = 'N'
       ; 
    l_dbg_msg VARCHAR2(100);               
    l_msg VARCHAR2(4000);   
    l_header_error_exists BOOLEAN;
    l_line_error_exists BOOLEAN;
BEGIN

     l_msg := null; 
     -- write error output file header
     --FND_FILE.PUT_LINE(FND_FILE.OUTPUT,l_msg); --todo uncomment in ZONE B VDI dev instance     
     l_dbg_msg := '12.0. Looping through error recs cursor.'; 
     FND_FILE.PUT_LINE(FND_FILE.LOG, '----------------------------');
     FND_FILE.PUT_LINE(FND_FILE.LOG, 'Error messages of each converted record on the current run:');
     FOR r_hdr_recs IN c_hdr_recs(p_instance_id) LOOP
         FND_FILE.PUT_LINE(FND_FILE.LOG, ''); 
         FND_FILE.PUT_LINE(FND_FILE.LOG, 'Record Header Id: ' || r_hdr_recs.RECORD_HEADER_ID); 
         FND_FILE.PUT_LINE(FND_FILE.LOG, 'Transaction Id: ' || r_hdr_recs.TRANSACTION_ID);
         FND_FILE.PUT_LINE(FND_FILE.LOG, 'Operation Id: ' || r_hdr_recs.OPERATION_ID);
         FND_FILE.PUT_LINE(FND_FILE.LOG, 'Header errors:');
         l_line_error_exists := false;
         l_header_error_exists := false;
         FOR r_hdr_erred_recs IN c_hdr_erred_recs(r_hdr_recs.record_header_id) LOOP             
         /*l_msg := substr(r_hdr_erred_recs .RECORD_HEADER_ID || ',' || r_hdr_erred_recs.TRANSACTION_ID || ',' || r_hdr_erred_recs.OPERATION_ID || ',' || r_hdr_erred_recs.DESCRIPTION || ',' || r_hdr_erred_recs.OPERATION_TYPE || ',' ||
                  r_hdr_erred_recs.start_date || ',' || r_hdr_erred_recs.quality_inspection_type || ',' || r_hdr_erred_recs.competency_name  || ',' || r_hdr_erred_recs.secondary_buyoff || ',' || r_hdr_erred_recs.occurrence_factor || ',' || r_hdr_erred_recs.status || ',' || 
                  MROI_TT_ERROR_UTIL_PKG.GET_ERROR_MSG(r_hdr_erred_recs.ERROR_CODE,'',r_hdr_erred_recs.ERROR_FIELD,r_hdr_erred_recs.ERROR_ATTRIBUTE1, 
                                                       r_hdr_erred_recs.ERROR_ATTRIBUTE2, r_hdr_erred_recs.ERROR_ATTRIBUTE3, 
                                                       r_hdr_erred_recs.ERROR_ATTRIBUTE4, r_hdr_erred_recs.ORA_ERR_CODE,r_hdr_erred_recs.ORA_ERR_MSG), 1, 4000); */
          
               FND_FILE.PUT_LINE(FND_FILE.LOG, 'Error Code: ' || r_hdr_erred_recs.ERROR_CODE);
               FND_FILE.PUT_LINE(FND_FILE.LOG, 'Error field: ' || r_hdr_erred_recs.ERROR_FIELD);
               FND_FILE.PUT_LINE(FND_FILE.LOG, 'Error Message: ' || MROI_TT_ERROR_UTIL_PKG.GET_ERROR_MSG(r_hdr_erred_recs.ERROR_CODE,'',r_hdr_erred_recs.ERROR_FIELD,r_hdr_erred_recs.ERROR_ATTRIBUTE1, 
                                                       r_hdr_erred_recs.ERROR_ATTRIBUTE2, r_hdr_erred_recs.ERROR_ATTRIBUTE3, 
                                                       r_hdr_erred_recs.ERROR_ATTRIBUTE4, r_hdr_erred_recs.ORA_ERR_CODE,r_hdr_erred_recs.ORA_ERR_MSG));
               l_header_error_exists := true;                                        
         END LOOP; 
         IF l_header_error_exists = false THEN
                  FND_FILE.PUT_LINE(FND_FILE.LOG, 'None!');
         END IF;
         
         -- write error output file details
         FND_FILE.PUT_LINE(FND_FILE.LOG, 'Line errors:');
         --FND_FILE.PUT_LINE(FND_FILE.OUTPUT, l_msg); --todo uncomment in ZONE B VDI dev instance
         FOR r_line_erred_recs IN c_line_erred_recs(r_hdr_recs.record_header_id) LOOP
                  /*l_msg := substr(r_line_erred_recs.RECORD_HEADER_ID || ',' || r_line_erred_recs.RECORD_LINE_ID || ',' || r_line_erred_recs.rr_schedule_sequence || ',' || r_line_erred_recs.rr_resource_type || ',' || r_line_erred_recs.rr_resource || ',' || r_line_erred_recs.rr_quantity || ',' ||
                                  r_line_erred_recs.rr_duration || ',' || r_line_erred_recs.mr_item || ',' ||
                                  r_line_erred_recs.mr_description || ',' || r_line_erred_recs.mr_quantity || ',' || r_line_erred_recs.rd_document_num || ',' ||
                                  MROI_TT_ERROR_UTIL_PKG.GET_ERROR_MSG(r_line_erred_recs.ERROR_CODE,'',r_line_erred_recs.ERROR_FIELD,r_line_erred_recs.ERROR_ATTRIBUTE1, 
                                                                       r_line_erred_recs.ERROR_ATTRIBUTE2, r_line_erred_recs.ERROR_ATTRIBUTE3, 
                                                                       r_line_erred_recs.ERROR_ATTRIBUTE4, r_line_erred_recs.ORA_ERR_CODE,r_line_erred_recs.ORA_ERR_MSG), 1, 4000);                         
                  -- write error output file details
                  FND_FILE.PUT_LINE(FND_FILE.OUTPUT,l_msg);*/ --todo uncomment in ZONE B VDI dev instance
                  FND_FILE.PUT_LINE(FND_FILE.LOG, 'Record line Id: ' || r_line_erred_recs.RECORD_LINE_ID);
                  FND_FILE.PUT_LINE(FND_FILE.LOG, 'Error field: ' || r_line_erred_recs.ERROR_FIELD);
                  FND_FILE.PUT_LINE(FND_FILE.LOG, 'Error Message: ' || MROI_TT_ERROR_UTIL_PKG.GET_ERROR_MSG(r_line_erred_recs.ERROR_CODE,'',r_line_erred_recs.ERROR_FIELD,r_line_erred_recs.ERROR_ATTRIBUTE1, 
                                                       r_line_erred_recs.ERROR_ATTRIBUTE2, r_line_erred_recs.ERROR_ATTRIBUTE3, 
                                                       r_line_erred_recs.ERROR_ATTRIBUTE4, r_line_erred_recs.ORA_ERR_CODE,r_line_erred_recs.ORA_ERR_MSG));
                  l_line_error_exists := true;                                     
         END LOOP;
         
         IF l_line_error_exists = false THEN
                  FND_FILE.PUT_LINE(FND_FILE.LOG, 'None!');
         END IF;         
     END LOOP;


EXCEPTION         
    WHEN OTHERS THEN         
             p_x_mroi_err_code := 1;
             p_x_mroi_err_msg := 'Error occured inside MROI_AHL_OPS_CONV_PKG.output_errors when '||l_dbg_msg||' - '||SQLERRM;
             FND_FILE.PUT_LINE(FND_FILE.LOG, p_x_mroi_err_msg);
END output_errors; 

/* *************************************************************************************
   Name: output_trans_status
   Object Type: Procedure
   Description:
   Input params: p_instance_id
                 p_x_mroi_err_code
                 p_x_mroi_err_msg
   Out params:
   -----------  ----------------------    ----   ------------
   06/10/2019   Yu Jian Lao           1.0    Initial Creation
   ************************************************************************************* */ 
PROCEDURE output_trans_status(p_instance_id IN VARCHAR2                       
                             ,p_x_mroi_err_code OUT NUMBER
                             ,p_x_mroi_err_msg OUT VARCHAR2) IS

    CURSOR c_trx_status_recs(v_instance_id IN NUMBER) IS
        SELECT stg.RECORD_HEADER_ID, stg.OPERATION_ID, stg.TRANSACTION_ID, trx_status.STATUS_CODE
        FROM MROI_TT_TRX_STATUS trx_status,
             MROI_AHL_OPS_HDR_STG stg 
        WHERE  trx_status.TRANSACTION_ID = stg.TRANSACTION_ID
        AND  stg.INSTANCE_ID = v_instance_id;


    l_dbg_msg VARCHAR2(100);               
    l_msg VARCHAR2(4000);   

BEGIN

     l_msg := null; 
     -- write error output file header     
     l_dbg_msg := '12.0. Looping through error recs cursor.'; 
     FOR r_trx_status_recs IN c_trx_status_recs(p_instance_id) LOOP             
         l_msg := substr(r_trx_status_recs .RECORD_HEADER_ID || ', ' || r_trx_status_recs.TRANSACTION_ID || ', ' || r_trx_status_recs.OPERATION_ID || ', ' ||  r_trx_status_recs.STATUS_CODE, 1, 4000);            
         -- write error output file details
         FND_FILE.PUT_LINE(FND_FILE.LOG, l_msg); --todo uncomment in ZONE B VDI dev instance         
     END LOOP;


EXCEPTION         
    WHEN OTHERS THEN         
             p_x_mroi_err_code := 1;
             p_x_mroi_err_msg := 'Error occured inside MROI_AHL_OPS_CONV_PKG.output_trans_status when '||l_dbg_msg||' - '||SQLERRM;

END output_trans_status;

   /* *************************************************************************************
   Name: validate_operations_records
   Object Type: Procedure
   Description:
   Input params: p_instance_id
                 p_x_mroi_err_code
                 p_x_mroi_err_msg
   Out params:

   Change History:
   Date         Name                      Ver    Modification
   -----------  ----------------------    ----   ------------
   05/15/2019   Ranadheer Reddy           1.0    Initial Creation
   ************************************************************************************* */
   PROCEDURE validate_operations_records(
               p_instance_id       IN NUMBER
              ,p_user_id           IN NUMBER
              ,p_error_count      OUT NUMBER
              ,p_t_ops_hdr_stg    OUT t_ops_hdr_stg_tbl_type
              ,p_x_mroi_err_code  OUT NUMBER
              ,p_x_mroi_err_msg   OUT VARCHAR2
              ,p_x_mimes_err_code OUT NUMBER
              ,p_x_mimes_err_msg  OUT VARCHAR2) IS

      -- required fields table cursor
      CURSOR c_invalid_operations(v_instance_id NUMBER) IS 
            SELECT xxmro.MROI_AHL_OPS_ERR_S.NEXTVAL AS ERROR_ID,  rec.TRANSACTION_ID,  rec."RECORD_LEVEL_ID", rec."RECORD_LEVEL_TYPE", rec."ERROR_FIELD", rec."ERROR_CODE", rec."ERROR_ATTRIBUTE1", rec."ERROR_ATTRIBUTE2",rec."ERROR_ATTRIBUTE3",rec."ERROR_ATTRIBUTE4",
                   '' as ORA_ERR_CODE, '' as ORA_ERR_MSG, '' as ERR_ADDRESSED_FLAG, '' as ERR_OLD_FLAG
            FROM(
                  -- Check for operation_id null values within headers 
                  SELECT INSTANCE_ID, TRANSACTION_ID, RECORD_HEADER_ID AS RECORD_LEVEL_ID, 'HEADER' AS RECORD_LEVEL_TYPE, 'OPERATION_ID' AS ERROR_FIELD, 13000 AS ERROR_CODE, --13000 -> ERROR_FIELD is required.
                   '' AS ERROR_ATTRIBUTE1, '' AS ERROR_ATTRIBUTE2 , '' AS ERROR_ATTRIBUTE3, '' AS ERROR_ATTRIBUTE4 
                    FROM MROI_AHL_OPS_HDR_STG
                   WHERE operation_id IS NULL
                     AND instance_id = v_instance_id
                   UNION  ALL
                  -- Check for occurrence_factor null values within headers 
                  SELECT INSTANCE_ID, TRANSACTION_ID, RECORD_HEADER_ID AS RECORD_LEVEL_ID, 'HEADER' AS RECORD_LEVEL_TYPE, 'OCCURRENCE_FACTOR' AS ERROR_FIELD, 13000 AS ERROR_CODE, --13000 -> ERROR_FIELD is required.
                   '' AS ERROR_ATTRIBUTE1, '' AS ERROR_ATTRIBUTE2 , '' AS ERROR_ATTRIBUTE3, '' AS ERROR_ATTRIBUTE4 
                    FROM MROI_AHL_OPS_HDR_STG
                   WHERE occurrence_factor IS NULL
                     AND instance_id = v_instance_id
                   UNION  ALL  
                  -- Check for description null values within headers 
                  SELECT INSTANCE_ID, TRANSACTION_ID, RECORD_HEADER_ID AS RECORD_LEVEL_ID, 'HEADER' AS RECORD_LEVEL_TYPE, 'DESCRIPTION' AS ERROR_FIELD, 13000 AS ERROR_CODE, --13000 -> ERROR_FIELD is required.
                   '' AS ERROR_ATTRIBUTE1, '' AS ERROR_ATTRIBUTE2 , '' AS ERROR_ATTRIBUTE3, '' AS ERROR_ATTRIBUTE4 
                    FROM MROI_AHL_OPS_HDR_STG
                   WHERE description IS NULL
                     AND instance_id = v_instance_id
                   UNION  ALL                    
                  -- Check for standard values within headers 
                  SELECT INSTANCE_ID, TRANSACTION_ID, RECORD_HEADER_ID AS RECORD_LEVEL_ID, 'HEADER' AS RECORD_LEVEL_TYPE, 'STANDARD' AS ERROR_FIELD, 11000 AS ERROR_CODE, --11000 -> Value Mismatch.
                   standard AS ERROR_ATTRIBUTE1, 'YES OR NO' AS ERROR_ATTRIBUTE2 , '' AS ERROR_ATTRIBUTE3, '' AS ERROR_ATTRIBUTE4 
                    FROM MROI_AHL_OPS_HDR_STG
                   WHERE standard IS NOT NULL
                     AND upper(standard) != 'NO' and upper(standard) != 'YES'
                     AND instance_id = v_instance_id  
                   UNION  ALL  
                  -- Check for competency_name null values within headers 
                  SELECT INSTANCE_ID, TRANSACTION_ID, RECORD_HEADER_ID AS RECORD_LEVEL_ID, 'HEADER' AS RECORD_LEVEL_TYPE, 'COMPETENCY_NAME' AS ERROR_FIELD, 13000 AS ERROR_CODE, --13000 -> ERROR_FIELD is required.
                   '' AS ERROR_ATTRIBUTE1, '' AS ERROR_ATTRIBUTE2 , '' AS ERROR_ATTRIBUTE3, '' AS ERROR_ATTRIBUTE4 
                    FROM MROI_AHL_OPS_HDR_STG
                   WHERE competency_name IS NULL
                     AND instance_id = v_instance_id
                   UNION  ALL
                  -- Check for the operation_id is in the correct format which seperated by four dots
                  SELECT INSTANCE_ID, TRANSACTION_ID, RECORD_HEADER_ID AS RECORD_LEVEL_ID, 'HEADER' AS RECORD_LEVEL_TYPE, 'OPERATION_ID' AS ERROR_FIELD, 15002 AS ERROR_CODE, --15002-> ERROR_FIELD is invalid value.
                   'Concatenation of four segments seperated by "."' AS ERROR_ATTRIBUTE1, '' AS ERROR_ATTRIBUTE2 , '' AS ERROR_ATTRIBUTE3, '' AS ERROR_ATTRIBUTE4 
                    FROM MROI_AHL_OPS_HDR_STG a
                   WHERE a.operation_id IS NOT NULL
                     AND instance_id = v_instance_id 
                     AND (substr(a.operation_id,1,instr(a.operation_id,'.',1)-1) IS NULL OR 
                          substr(a.operation_id,instr(a.operation_id,'.',1)+1,instr(a.operation_id,'.',1,2)-instr(a.operation_id,'.',1)-1) IS NULL OR
                          substr(a.operation_id,instr(a.operation_id,'.',1,2)+1,instr(a.operation_id,'.',1,3)-instr(a.operation_id,'.',1,2)-1) IS NULL OR 
                          substr(a.operation_id,instr(a.operation_id,'.',1,3)+1) IS NULL)
                  UNION  ALL
                  -- Check for the operation_id already exists within the system
                  SELECT INSTANCE_ID, TRANSACTION_ID, RECORD_HEADER_ID AS RECORD_LEVEL_ID, 'HEADER' AS RECORD_LEVEL_TYPE, 'OPERATION_ID' AS ERROR_FIELD, 10004 AS ERROR_CODE, --10004-> ERROR_FIELD is already exists.
                   operation_id AS ERROR_ATTRIBUTE1, '' AS ERROR_ATTRIBUTE2 , '' AS ERROR_ATTRIBUTE3, '' AS ERROR_ATTRIBUTE4 
                    FROM MROI_AHL_OPS_HDR_STG a
                   WHERE a.operation_id IS NOT NULL
                     AND instance_id = v_instance_id 
                     AND exists (select 1 from ahl_operations_b_kfv b where a.operation_id = b.concatenated_segments)       
                 UNION  ALL 
                  -- Check for operation type value exists in lookup
                  SELECT INSTANCE_ID, TRANSACTION_ID, RECORD_HEADER_ID AS RECORD_LEVEL_ID, 'HEADER' AS RECORD_LEVEL_TYPE, 'OPERATION_TYPE' AS ERROR_FIELD, 10005 AS ERROR_CODE, --10005 -> lookup error.
                   operation_Type AS ERROR_ATTRIBUTE1, 'Application Object Library Lookups: AHL_OPERATION_TYPE' AS ERROR_ATTRIBUTE2 , '' AS ERROR_ATTRIBUTE3, '' AS ERROR_ATTRIBUTE4 
                   FROM MROI_AHL_OPS_HDR_STG a
                  WHERE operation_Type IS NOT NULL
                    AND instance_id = v_instance_id
                    AND not exists (select 1 from fnd_lookup_values b where b.lookup_type = 'AHL_OPERATION_TYPE' and b.meaning = a.operation_type and enabled_flag = 'Y')
                  UNION  ALL 
                  -- Check for quality inspection type exists in collection element
                  SELECT INSTANCE_ID, TRANSACTION_ID, RECORD_HEADER_ID AS RECORD_LEVEL_ID, 'HEADER' AS RECORD_LEVEL_TYPE, 'QUALITY_INSPECTION_TYPE' AS ERROR_FIELD, 10005 AS ERROR_CODE, --10005 -> lookup error.
                   QUALITY_INSPECTION_TYPE AS ERROR_ATTRIBUTE1, 'Collection Element as an Inspection Type value' AS ERROR_ATTRIBUTE2 , '' AS ERROR_ATTRIBUTE3, '' AS ERROR_ATTRIBUTE4 
                   FROM MROI_AHL_OPS_HDR_STG a
                  WHERE quality_inspection_type IS NOT NULL
                    AND instance_id = v_instance_id
                    AND not exists (select 1 from qa_char_value_lookups b where b.char_id = 87 and b.description = a.quality_inspection_type) 
                  UNION  ALL 
                  -- Check for competency name exists in HR competences
                  SELECT INSTANCE_ID, TRANSACTION_ID, RECORD_HEADER_ID AS RECORD_LEVEL_ID, 'HEADER' AS RECORD_LEVEL_TYPE, 'COMPETENCY_NAME' AS ERROR_FIELD, 10005 AS ERROR_CODE, --10005 -> lookup error.
                   competency_name AS ERROR_ATTRIBUTE1, 'HR Competences' AS ERROR_ATTRIBUTE2 , '' AS ERROR_ATTRIBUTE3, '' AS ERROR_ATTRIBUTE4 
                   FROM MROI_AHL_OPS_HDR_STG a
                  WHERE competency_name IS NOT NULL
                    AND instance_id = v_instance_id
                    AND not exists (select 1 from PER_COMPETENCES b where b.name = a.competency_name and trunc(sysdate) between nvl(date_from,sysdate) and nvl(date_to,sysdate))  
                  UNION  ALL
                  -- Check for secondary buyoff exists within the secondary buyoff valueset in the system
                  SELECT a.INSTANCE_ID, a.TRANSACTION_ID, a.RECORD_HEADER_ID AS RECORD_LEVEL_ID, 'HEADER' AS RECORD_LEVEL_TYPE, 'SECONDARY_BUYOFF' AS ERROR_FIELD, 10005 AS ERROR_CODE, --10005 -> lookup error.
                   a.secondary_buyoff AS ERROR_ATTRIBUTE1, 'value set Secondary Buyoff' AS ERROR_ATTRIBUTE2 , '' AS ERROR_ATTRIBUTE3, '' AS ERROR_ATTRIBUTE4 
                   FROM MROI_AHL_OPS_HDR_STG a
                  WHERE a.secondary_buyoff IS NOT NULL
                    AND a.instance_id = v_instance_id
                    AND not exists (select 1 FROM fnd_flex_value_sets c, fnd_flex_values d 
                                              where flex_value_set_name = 'Secondary Buyoff' 
                                                and c.flex_value_set_id = d.flex_value_set_id
                                                and d.flex_value = a.secondary_buyoff
                                                and d.enabled_flag = 'Y'
                                             )                
                  UNION  ALL 
                  -- Check for rr schedule sequence null values within lines
                  SELECT a.INSTANCE_ID, a.TRANSACTION_ID, b.RECORD_LINE_ID AS RECORD_LEVEL_ID, 'LINE' AS RECORD_LEVEL_TYPE, 'RR_SCHEDULE_SEQUENCE' AS ERROR_FIELD, 13000 AS ERROR_CODE, --13000 -> ERROR_FIELD is required.
                   '' AS ERROR_ATTRIBUTE1, '' AS ERROR_ATTRIBUTE2 , '' AS ERROR_ATTRIBUTE3, '' AS ERROR_ATTRIBUTE4 
                   FROM MROI_AHL_OPS_HDR_STG a, MROI_AHL_OPS_LINE_STG b
                  WHERE a.instance_id = v_instance_id
                    AND a.record_header_id = b.record_header_id
                    AND b.rr_schedule_sequence is null 
                    AND (b.rr_resource_type IS NOT NULL OR b.rr_resource IS NOT NULL OR b.rr_quantity IS NOT NULL or b.rr_duration IS NOT NULL)
                  UNION  ALL
                  -- Check for quantity null values within lines
                  SELECT a.INSTANCE_ID, a.TRANSACTION_ID, b.RECORD_LINE_ID AS RECORD_LEVEL_ID, 'LINE' AS RECORD_LEVEL_TYPE, 'RR_QUANTITY' AS ERROR_FIELD, 13000 AS ERROR_CODE, --13000 -> ERROR_FIELD is required.
                   '' AS ERROR_ATTRIBUTE1, '' AS ERROR_ATTRIBUTE2 , '' AS ERROR_ATTRIBUTE3, '' AS ERROR_ATTRIBUTE4 
                   FROM MROI_AHL_OPS_HDR_STG a, MROI_AHL_OPS_LINE_STG b
                  WHERE a.instance_id = v_instance_id
                    AND a.record_header_id = b.record_header_id
                    AND b.rr_quantity is null 
                    AND (b.rr_resource_type IS NOT NULL OR b.rr_resource IS NOT NULL OR b.rr_schedule_sequence IS NOT NULL or b.rr_duration IS NOT NULL)
                  UNION  ALL
                  -- Check for rr schedule sequence null values within lines
                  SELECT a.INSTANCE_ID, a.TRANSACTION_ID, b.RECORD_LINE_ID AS RECORD_LEVEL_ID, 'LINE' AS RECORD_LEVEL_TYPE, 'RR_DURATION' AS ERROR_FIELD, 13000 AS ERROR_CODE, --13000 -> ERROR_FIELD is required.
                   '' AS ERROR_ATTRIBUTE1, '' AS ERROR_ATTRIBUTE2 , '' AS ERROR_ATTRIBUTE3, '' AS ERROR_ATTRIBUTE4 
                   FROM MROI_AHL_OPS_HDR_STG a, MROI_AHL_OPS_LINE_STG b
                  WHERE a.instance_id = v_instance_id
                    AND a.record_header_id = b.record_header_id
                    AND b.rr_duration is null 
                    AND (b.rr_resource_type IS NOT NULL OR b.rr_resource IS NOT NULL OR b.rr_schedule_sequence IS NOT NULL or b.rr_quantity IS NOT NULL)
                  UNION  ALL
                  -- Check for mr item null values within lines
                  SELECT a.INSTANCE_ID, a.TRANSACTION_ID, b.RECORD_LINE_ID AS RECORD_LEVEL_ID, 'LINE' AS RECORD_LEVEL_TYPE, 'MR_ITEM' AS ERROR_FIELD, 13000 AS ERROR_CODE, --13000 -> ERROR_FIELD is required.
                   '' AS ERROR_ATTRIBUTE1, '' AS ERROR_ATTRIBUTE2 , '' AS ERROR_ATTRIBUTE3, '' AS ERROR_ATTRIBUTE4 
                   FROM MROI_AHL_OPS_HDR_STG a, MROI_AHL_OPS_LINE_STG b
                  WHERE a.instance_id = v_instance_id
                    AND a.record_header_id = b.record_header_id
                    AND b.mr_item is null 
                    AND b.mr_quantity IS NOT NULL
                  UNION  ALL
                  -- Check for mr quantity null values within lines
                  SELECT a.INSTANCE_ID, a.TRANSACTION_ID, b.RECORD_LINE_ID AS RECORD_LEVEL_ID, 'LINE' AS RECORD_LEVEL_TYPE, 'MR_QUANTITY' AS ERROR_FIELD, 13000 AS ERROR_CODE, --13000 -> ERROR_FIELD is required.
                   '' AS ERROR_ATTRIBUTE1, '' AS ERROR_ATTRIBUTE2 , '' AS ERROR_ATTRIBUTE3, '' AS ERROR_ATTRIBUTE4 
                   FROM MROI_AHL_OPS_HDR_STG a, MROI_AHL_OPS_LINE_STG b
                  WHERE a.instance_id = v_instance_id
                    AND a.record_header_id = b.record_header_id
                    AND b.mr_quantity is null 
                    AND b.mr_item IS NOT NULL     
                  UNION  ALL  
                  -- Check for document number exists as document index within the system
                  SELECT a.INSTANCE_ID, a.TRANSACTION_ID, b.RECORD_LINE_ID AS RECORD_LEVEL_ID, 'LINE' AS RECORD_LEVEL_TYPE, 'RD_DOCUMENT_NUM' AS ERROR_FIELD, 10005 AS ERROR_CODE, --10005 -> lookup error.
                   b.rd_document_num AS ERROR_ATTRIBUTE1, 'Document Index as a Document Number' AS ERROR_ATTRIBUTE2 , '' AS ERROR_ATTRIBUTE3, '' AS ERROR_ATTRIBUTE4 
                   FROM MROI_AHL_OPS_HDR_STG a, MROI_AHL_OPS_LINE_STG b
                  WHERE b.rd_document_num IS NOT NULL
                    AND a.instance_id = v_instance_id
                    AND a.record_header_id = b.record_header_id
                    AND not exists (select 1 from ahl_documents_b c where c.document_no = b.rd_document_num) 
                  UNION  ALL 
                  -- Check for resource exists as CMRO resource within the system
                  SELECT a.INSTANCE_ID, a.TRANSACTION_ID, b.RECORD_LINE_ID AS RECORD_LEVEL_ID, 'LINE' AS RECORD_LEVEL_TYPE, 'RR_RESOURCE_TYPE' AS ERROR_FIELD, 10005 AS ERROR_CODE, --10005 -> lookup error.
                   b.rr_resource AS ERROR_ATTRIBUTE1, 'BOM Resource Type' AS ERROR_ATTRIBUTE2 , '' AS ERROR_ATTRIBUTE3, '' AS ERROR_ATTRIBUTE4 
                   FROM MROI_AHL_OPS_HDR_STG a, MROI_AHL_OPS_LINE_STG b
                  WHERE b.rr_resource_type IS NOT NULL
                    AND a.instance_id = v_instance_id
                    AND a.record_header_id = b.record_header_id
                    AND not exists (select 1 from fnd_lookup_values c where c.lookup_type = 'BOM_RESOURCE_TYPE' and upper(c.meaning) = upper(b.rr_resource_type)) 
                  UNION  ALL
                  -- Check for resource exists as CMRO resource within the system
                  SELECT a.INSTANCE_ID, a.TRANSACTION_ID, b.RECORD_LINE_ID AS RECORD_LEVEL_ID, 'LINE' AS RECORD_LEVEL_TYPE, 'RR_RESOURCE' AS ERROR_FIELD, 10005 AS ERROR_CODE, --10005 -> lookup error.
                   b.rr_resource AS ERROR_ATTRIBUTE1, 'CRMO Resource' AS ERROR_ATTRIBUTE2 , '' AS ERROR_ATTRIBUTE3, '' AS ERROR_ATTRIBUTE4 
                   FROM MROI_AHL_OPS_HDR_STG a, MROI_AHL_OPS_LINE_STG b
                  WHERE b.rr_resource IS NOT NULL
                    AND a.instance_id = v_instance_id
                    AND a.record_header_id = b.record_header_id
                    AND not exists (select 1 from ahl_resources c where c.name = b.rr_resource) 
                  UNION  ALL 
                  -- Check for material requirement exists as item in the system
                  SELECT a.INSTANCE_ID, a.TRANSACTION_ID, b.RECORD_LINE_ID AS RECORD_LEVEL_ID, 'LINE' AS RECORD_LEVEL_TYPE, 'MR_ITEM' AS ERROR_FIELD, 10005 AS ERROR_CODE, --10005 -> lookup error.
                   b.mr_item AS ERROR_ATTRIBUTE1, 'Inventory Item and must be assigned to one or more organization other than the Master organization' AS ERROR_ATTRIBUTE2 , '' AS ERROR_ATTRIBUTE3, '' AS ERROR_ATTRIBUTE4 
                   FROM MROI_AHL_OPS_HDR_STG a, MROI_AHL_OPS_LINE_STG b
                  WHERE b.mr_item IS NOT NULL
                    AND a.instance_id = v_instance_id
                    AND a.record_header_id = b.record_header_id
                    AND not exists (select 1 FROM mtl_system_items_b_kfv msi,
                                                  mtl_parameters mp
                                            WHERE mp.organization_code <> 'MST'
                                              AND msi.concatenated_segments = b.mr_item
                                              AND msi.organization_id = mp.organization_id)                    
                  UNION  ALL
                  -- Check for document number duplication within lines 
                  SELECT a.INSTANCE_ID, a.TRANSACTION_ID, b.RECORD_LINE_ID AS RECORD_LEVEL_ID, 'LINE' AS RECORD_LEVEL_TYPE, 'RD_DOCUMENT_NUM' AS ERROR_FIELD, 10008 AS ERROR_CODE, --10008 -> lookup error.
                   'operation' AS ERROR_ATTRIBUTE1, 'an operation' AS ERROR_ATTRIBUTE2 , 'Document Index' AS ERROR_ATTRIBUTE3, '' AS ERROR_ATTRIBUTE4 
                   FROM MROI_AHL_OPS_HDR_STG a, MROI_AHL_OPS_LINE_STG b
                  WHERE b.rd_document_num IS NOT NULL
                    AND a.instance_id = v_instance_id
                    AND a.record_header_id = b.record_header_id
                    AND exists (select 1 from MROI_AHL_OPS_LINE_STG c where c.record_header_id = a.record_header_id and c.record_line_id != b.record_line_id and c.rd_document_num = b.rd_document_num) 
                  UNION  ALL 
                  -- Check for resource duplication within lines
                  SELECT a.INSTANCE_ID, a.TRANSACTION_ID, b.RECORD_LINE_ID AS RECORD_LEVEL_ID, 'LINE' AS RECORD_LEVEL_TYPE, 'RR_RESOURCE' AS ERROR_FIELD, 10008 AS ERROR_CODE, --10008 -> lookup error.
                   'operation' AS ERROR_ATTRIBUTE1, 'an operation' AS ERROR_ATTRIBUTE2 , 'CMRO Resource' AS ERROR_ATTRIBUTE3, '' AS ERROR_ATTRIBUTE4 
                   FROM MROI_AHL_OPS_HDR_STG a, MROI_AHL_OPS_LINE_STG b
                  WHERE b.rr_resource IS NOT NULL
                    AND a.instance_id = v_instance_id
                    AND a.record_header_id = b.record_header_id
                    AND exists (select 1 from MROI_AHL_OPS_LINE_STG c where c.record_header_id = a.record_header_id and c.record_line_id != b.record_line_id and c.rr_resource = b.rr_resource) 
                  UNION  ALL 
                  -- Check for mr_item duplication within lines
                  SELECT a.INSTANCE_ID, a.TRANSACTION_ID, b.RECORD_LINE_ID AS RECORD_LEVEL_ID, 'LINE' AS RECORD_LEVEL_TYPE, 'MR_ITEM' AS ERROR_FIELD, 10008 AS ERROR_CODE, --10008 -> lookup error.
                   'operation' AS ERROR_ATTRIBUTE1, 'an operation' AS ERROR_ATTRIBUTE2 , 'Inventory Item' AS ERROR_ATTRIBUTE3, '' AS ERROR_ATTRIBUTE4 
                   FROM MROI_AHL_OPS_HDR_STG a, MROI_AHL_OPS_LINE_STG b
                  WHERE b.mr_item IS NOT NULL
                    AND a.instance_id = v_instance_id
                    AND a.record_header_id = b.record_header_id
                    AND exists (select 1 from MROI_AHL_OPS_LINE_STG c where c.record_header_id = a.record_header_id and c.record_line_id != b.record_line_id and c.mr_item = b.mr_item) 
                  UNION  ALL
                  -- Check to ensure at least one person type resource exists within lines
                  SELECT a.INSTANCE_ID, a.TRANSACTION_ID, b.RECORD_LINE_ID AS RECORD_LEVEL_ID, 'LINE' AS RECORD_LEVEL_TYPE, 'RR_RESOURCE_TYPE' AS ERROR_FIELD, 10009 AS ERROR_CODE, --10009 -> lookup error.
                   '' AS ERROR_ATTRIBUTE1, '' AS ERROR_ATTRIBUTE2 , '' AS ERROR_ATTRIBUTE3, '' AS ERROR_ATTRIBUTE4 
                   FROM MROI_AHL_OPS_HDR_STG a, MROI_AHL_OPS_LINE_STG b
                  WHERE a.instance_id = v_instance_id
                    AND a.record_header_id = b.record_header_id
                    AND not exists (select 1 from MROI_AHL_OPS_LINE_STG c where c.record_header_id = a.record_header_id and upper(c.rr_resource_type) = 'PERSON' and rr_resource IS NOT NULL and rownum = 1)
                    AND rownum = 1 
                  UNION  ALL
                  -- Check to ensure rr duration is in numeric format
                  SELECT a.INSTANCE_ID, a.TRANSACTION_ID, a.RECORD_HEADER_ID AS RECORD_LEVEL_ID, 'HEADER' AS RECORD_LEVEL_TYPE, 'OCCURRENCE_FACTOR' AS ERROR_FIELD, 12003 AS ERROR_CODE, --12003 -> format error.
                   '' AS ERROR_ATTRIBUTE1, '' AS ERROR_ATTRIBUTE2 , '' AS ERROR_ATTRIBUTE3, '' AS ERROR_ATTRIBUTE4 
                   FROM MROI_AHL_OPS_HDR_STG a
                  WHERE a.instance_id = v_instance_id
                    AND a.occurrence_factor IS NOT NULL
                    AND is_numeric(a.occurrence_factor) = 0  
                  UNION  ALL
                  -- Check to ensure schedule sequence is in numeric format
                  SELECT a.INSTANCE_ID, a.TRANSACTION_ID, b.RECORD_LINE_ID AS RECORD_LEVEL_ID, 'LINE' AS RECORD_LEVEL_TYPE, 'RR_SCHEDULE_SEQUENCE' AS ERROR_FIELD, 12003 AS ERROR_CODE, --12003 -> format error.
                   '' AS ERROR_ATTRIBUTE1, '' AS ERROR_ATTRIBUTE2 , '' AS ERROR_ATTRIBUTE3, '' AS ERROR_ATTRIBUTE4 
                   FROM MROI_AHL_OPS_HDR_STG a, MROI_AHL_OPS_LINE_STG b
                  WHERE a.instance_id = v_instance_id
                    AND a.record_header_id = b.record_header_id
                    AND b.rr_schedule_sequence IS NOT NULL
                    AND is_numeric(b.rr_schedule_sequence) = 0
                  UNION  ALL 
                  -- Check to ensure rr quantity is in numeric format
                  SELECT a.INSTANCE_ID, a.TRANSACTION_ID, b.RECORD_LINE_ID AS RECORD_LEVEL_ID, 'LINE' AS RECORD_LEVEL_TYPE, 'RR_QUANTITY' AS ERROR_FIELD, 12003 AS ERROR_CODE, --12003 -> format error.
                   '' AS ERROR_ATTRIBUTE1, '' AS ERROR_ATTRIBUTE2 , '' AS ERROR_ATTRIBUTE3, '' AS ERROR_ATTRIBUTE4 
                   FROM MROI_AHL_OPS_HDR_STG a, MROI_AHL_OPS_LINE_STG b
                  WHERE a.instance_id = v_instance_id
                    AND a.record_header_id = b.record_header_id
                    AND b.rr_quantity IS NOT NULL
                    AND is_numeric(b.rr_quantity) = 0
                  UNION  ALL 
                  -- Check to ensure rr duration is in numeric format
                  SELECT a.INSTANCE_ID, a.TRANSACTION_ID, b.RECORD_LINE_ID AS RECORD_LEVEL_ID, 'LINE' AS RECORD_LEVEL_TYPE, 'RR_DURATION' AS ERROR_FIELD, 12003 AS ERROR_CODE, --12003 -> format error.
                   '' AS ERROR_ATTRIBUTE1, '' AS ERROR_ATTRIBUTE2 , '' AS ERROR_ATTRIBUTE3, '' AS ERROR_ATTRIBUTE4 
                   FROM MROI_AHL_OPS_HDR_STG a, MROI_AHL_OPS_LINE_STG b
                  WHERE a.instance_id = v_instance_id
                    AND a.record_header_id = b.record_header_id
                    AND b.rr_duration IS NOT NULL
                    AND is_numeric(b.rr_duration) = 0  
                  UNION  ALL 
                  -- Check to ensure mr quantity is in numeric format
                  SELECT a.INSTANCE_ID, a.TRANSACTION_ID, b.RECORD_LINE_ID AS RECORD_LEVEL_ID, 'LINE' AS RECORD_LEVEL_TYPE, 'MR_QUANTITY' AS ERROR_FIELD, 12003 AS ERROR_CODE, --12003 -> format error.
                   '' AS ERROR_ATTRIBUTE1, '' AS ERROR_ATTRIBUTE2 , '' AS ERROR_ATTRIBUTE3, '' AS ERROR_ATTRIBUTE4 
                   FROM MROI_AHL_OPS_HDR_STG a, MROI_AHL_OPS_LINE_STG b
                  WHERE a.instance_id = v_instance_id
                    AND a.record_header_id = b.record_header_id
                    AND b.mr_quantity IS NOT NULL
                    AND is_numeric(b.mr_quantity) = 0
            ) rec
            WHERE rec.INSTANCE_ID = v_instance_id
            --order by transaction_id
      ;  

      -- required fields table cursor
      CURSOR c_valid_operations(v_instance_id NUMBER) IS
            SELECT hdr.record_header_id, 
                   hdr.transaction_id, 
                   hdr.instance_id, 
                   hdr.conc_request_id, 
                   hdr.operation_id, 
                   hdr.description,
                   hdr.operation_type, 
                   hdr.start_date,
                   hdr.quality_inspection_type,
                   hdr.standard,
                   hdr.competency_name, 
                   hdr.secondary_buyoff,
                   hdr.occurrence_factor,
                   hdr.creation_date,
                   hdr.created_by,
                   hdr.last_update_date,
                   hdr.last_updated_by,
                   'Validated' status,
                   null error_message
              FROM MROI_AHL_OPS_HDR_STG hdr
             WHERE hdr.instance_id = v_instance_id
               AND NOT EXISTS (SELECT 1 FROM MROI_AHL_OPS_ERR err WHERE  hdr.transaction_id = err.transaction_id and err_old_flag = 'N');  
      l_dummy                     VARCHAR2(1);
      l_status                    VARCHAR2(15);
      l_error_msg                 VARCHAR2(4000);
      l_dbg_msg                   VARCHAR2(300);
      l_document_id               NUMBER;
      l_resource_id               NUMBER;
      l_item_id                   NUMBER;
      l_segment1                  VARCHAR2(30);
      l_segment2                  VARCHAR2(30);
      l_segment3                  VARCHAR2(30);
      l_segment4                  VARCHAR2(30);

      l_rec_count                 NUMBER := 0;
      l_t_err_tbl  MROI_TT_ERROR_UTIL_PKG.t_err_tbl := MROI_TT_ERROR_UTIL_PKG.t_err_tbl(); --table type variable for err table
      l_trx_error_status_tbl MROI_TT_STATUS_UTIL_PKG.t_trx_status_tbl := MROI_TT_STATUS_UTIL_PKG.t_trx_status_tbl(); --table type variable for trx status table  
      l_trx_valid_status_tbl MROI_TT_STATUS_UTIL_PKG.t_trx_status_tbl := MROI_TT_STATUS_UTIL_PKG.t_trx_status_tbl(); --table type variable for trx status table  
      l_t_ops_hdr_stg  t_ops_hdr_stg_tbl_type := t_ops_hdr_stg_tbl_type();  --table type variable for trx status table 

      l_status_code               NUMBER;
      l_x_mimes_err_code          NUMBER;
      l_x_mimes_err_msg           VARCHAR2(1000);
      l_transaction_id            NUMBER := null;

      e_mimes_exception EXCEPTION;  
      e_proc_exception EXCEPTION;   
   BEGIN
      p_x_mroi_err_code := 0;
      p_x_mroi_err_msg := NULL;
      p_x_mimes_err_code := 0;
      p_x_mimes_err_msg := NULL;
      p_error_count := 0;

      -- open cursor
      l_dbg_msg := '0011. Openning c_invalid_items coursor. ';
      /*FOR r_invalid_operations IN c_invalid_operations(p_instance_id) LOOP
            -- assign error to the error table variable
            l_t_err_tbl.EXTEND;
            --l_t_err_tbl(l_t_err_tbl.COUNT).ERROR_ID := xxmro.MROI_AHL_OPS_ERR_S.NEXTVAL;
            l_t_err_tbl(l_t_err_tbl.COUNT).TRANSACTION_ID := r_invalid_operations.TRANSACTION_ID;
            l_t_err_tbl(l_t_err_tbl.COUNT).RECORD_LEVEL_ID := r_invalid_operations.RECORD_LEVEL_ID;
            l_t_err_tbl(l_t_err_tbl.COUNT).RECORD_LEVEL_TYPE := r_invalid_operations.RECORD_LEVEL_TYPE; 
            l_t_err_tbl(l_t_err_tbl.COUNT).ERROR_FIELD := r_invalid_operations.ERROR_FIELD; 
            l_t_err_tbl(l_t_err_tbl.COUNT).ERROR_CODE := r_invalid_operations.ERROR_CODE;
            l_t_err_tbl(l_t_err_tbl.COUNT).ERROR_ATTRIBUTE1 := r_invalid_operations.ERROR_ATTRIBUTE1;
            l_t_err_tbl(l_t_err_tbl.COUNT).ERROR_ATTRIBUTE2 := r_invalid_operations.ERROR_ATTRIBUTE2;
            l_t_err_tbl(l_t_err_tbl.COUNT).ERROR_ATTRIBUTE3 := r_invalid_operations.ERROR_ATTRIBUTE3;
            l_t_err_tbl(l_t_err_tbl.COUNT).ERROR_ATTRIBUTE4 := r_invalid_operations.ERROR_ATTRIBUTE4;

            -- assign error status code to the trx status table variable
            IF l_transaction_id IS NULL or l_transaction_id != r_invalid_operations.transaction_id THEN
                l_trx_error_status_tbl.EXTEND; 
                l_trx_error_status_tbl(l_trx_error_status_tbl.COUNT).STATUS_CODE := MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_ERRED_STG;
                l_trx_error_status_tbl(l_trx_error_status_tbl.COUNT).TRANSACTION_ID := r_invalid_operations.TRANSACTION_ID;
                l_transaction_id := r_invalid_operations.transaction_id;
            END IF;                                   
      END LOOP;*/
      
      OPEN c_invalid_operations(p_instance_id);
      FETCH c_invalid_operations BULK COLLECT INTO l_t_err_tbl;
      select distinct transaction_id, 
             -1 as instance_id,
             NULL as reprocess_instance_id,
             MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_ERRED_STG as status_code, 
             '' as reject_reason, 
             '' as remarks, '' as oit_reject_reason, 
             '' as oit_remaks, sysdate as creation_date, 
             -1 as created_by,
             sysdate as last_update_date, 
             -1 as last_updated_by
             bulk collect into l_trx_error_status_tbl from table(l_t_err_tbl);
       
      IF l_t_err_tbl.COUNT > 0 THEN

          -- call MIMES API to insert collection of erred recs into MROI_AHL_OPS_ERR table
          l_dbg_msg := '0012. Calling MROI_TT_ERROR_UTIL_PKG.log_error_batch for instance ID: '||p_instance_id;                                        
          MROI_TT_ERROR_UTIL_PKG.log_error_batch(p_instance_id       => p_instance_id
                                                ,p_user_id           => p_user_id
                                                ,p_t_err_tbl         => l_t_err_tbl 
                                                ,p_x_mimes_err_code  => l_x_mimes_err_code
                                                ,p_x_mimes_err_msg   => l_x_mimes_err_msg);

          IF l_x_mimes_err_code = 1 THEN                                            
              RAISE e_mimes_exception;                                             
          END IF;

          l_dbg_msg := '0013. Calling MROI_TT_STATUS_UTIL_PKG.update_trx_status_batch for instance ID: '||p_instance_id;  
          MROI_TT_STATUS_UTIL_PKG.update_trx_status_batch(p_trx_status_tbl    => l_trx_error_status_tbl  
                                                         ,p_user_id           => p_user_id                                                               
                                                         ,p_x_mimes_err_code  => l_x_mimes_err_code
                                                         ,p_x_mimes_err_msg   => l_x_mimes_err_msg);
           IF l_x_mimes_err_code = 1 THEN                                            
                RAISE e_mimes_exception;                                             
           END IF;

           FORALL j IN l_trx_error_status_tbl.FIRST..l_trx_error_status_tbl.LAST
               update mroi_ahl_ops_hdr_stg set status = 'Error' where transaction_id = l_trx_error_status_tbl(j).transaction_id;
           COMMIT;
           l_dbg_msg := '0014. Calling output_errors to write errors to output file. ';
           output_errors(p_instance_id     => p_instance_id                      
                        ,p_x_mroi_err_code => p_x_mroi_err_code
                        ,p_x_mroi_err_msg  => p_x_mroi_err_msg);                      
      END IF;


      l_t_err_tbl.DELETE; 
      l_trx_error_status_tbl.DELETE;      

      l_dbg_msg := '0015. Getting validation transaction record error count for instance ID: '||p_instance_id;  
      SELECT COUNT(*)
      INTO p_error_count
      FROM MROI_TT_TRX_STATUS   
      WHERE STATUS_CODE = MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_ERRED_STG
      AND INSTANCE_ID =  p_instance_id; 

      -- Update the rd_document_id, rr_resrouce_id and mr_item_id columns
      UPDATE xxmro.mroi_ahl_ops_line_stg a
         SET rd_document_id = (SELECT document_id
                                FROM ahl_documents_b
                               WHERE document_no = a.rd_document_num), 
             rr_resource_id = (SELECT resource_id 
                                FROM ahl_resources b
                               WHERE b.name = a.rr_resource),
             mr_item_id = (SELECT inventory_item_id
                            FROM mtl_system_items_b_kfv msi,
                                 mtl_parameters mp
                           WHERE mp.organization_code = 'MST'
                             AND msi.concatenated_segments = a.mr_item
                             AND msi.organization_id = mp.organization_id)
       WHERE record_header_id in (select record_header_id from MROI_AHL_OPS_HDR_STG b where instance_id = p_instance_id) ;

      l_dbg_msg := '0016. Openning r_valid_operations coursor. ';
      OPEN c_valid_operations(p_instance_id);
      FETCH c_valid_operations BULK COLLECT INTO l_t_ops_hdr_stg;
      select distinct transaction_id, 
             -1 as instance_id,
             NULL as reprocess_instance_id,
             MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_LOADED_OIT_API as status_code, 
             '' as reject_reason, 
             '' as remarks, '' as oit_reject_reason, 
             '' as oit_remaks, sysdate as creation_date, 
             -1 as created_by,
             sysdate as last_update_date, 
             -1 as last_updated_by
             bulk collect into l_trx_valid_status_tbl from table(l_t_ops_hdr_stg);
      
      /*FOR r_valid_operations IN c_valid_operations(p_instance_id) LOOP
             l_trx_valid_status_tbl.EXTEND; 
             l_trx_valid_status_tbl(l_trx_valid_status_tbl.COUNT).STATUS_CODE := MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_LOADED_OIT_API;
             l_trx_valid_status_tbl(l_trx_valid_status_tbl.COUNT).TRANSACTION_ID := r_valid_operations.TRANSACTION_ID; 
             l_t_ops_hdr_stg.EXTEND;
             l_t_ops_hdr_stg(l_t_ops_hdr_stg.COUNT).record_header_id := r_valid_operations.record_header_id;
             l_t_ops_hdr_stg(l_t_ops_hdr_stg.COUNT).transaction_id := r_valid_operations.transaction_id;
             l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).instance_id := r_valid_operations.instance_id;
             l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).conc_request_id := r_valid_operations.conc_request_id;
             l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).operation_id := r_valid_operations.operation_id;
             l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).description := r_valid_operations.description;
             l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).operation_type := r_valid_operations.operation_type;
             l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).start_date := r_valid_operations.start_date;
             l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).quality_inspection_type := r_valid_operations.quality_inspection_type;
             l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).standard := r_valid_operations.standard;
             l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).competency_name := r_valid_operations.competency_name ;
             l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).secondary_buyoff := r_valid_operations.secondary_buyoff ;
             l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).occurrence_factor := r_valid_operations.occurrence_factor ;
             l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).creation_date := r_valid_operations.creation_Date;
             l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).created_by := r_valid_operations.created_by;
             l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).last_update_date := r_valid_operations.last_update_date;
             l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).last_updated_by := r_valid_operations.last_updated_by;
             l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).status := 'Validated';
             l_t_ops_hdr_stg(l_t_ops_hdr_stg.count).error_message := NULL;
      END LOOP;*/

      -- Update the status column
      FORALL i IN l_t_ops_hdr_stg.FIRST..l_t_ops_hdr_stg.LAST
             update mroi_ahl_ops_hdr_stg set status = l_t_ops_hdr_stg(i).status where record_header_id = l_t_ops_hdr_stg(i).record_header_id;
      COMMIT;

      p_t_ops_hdr_stg := l_t_ops_hdr_stg; 
      l_dbg_msg := '0017. Calling MROI_TT_STATUS_UTIL_PKG.update_trx_status_batch for instance ID: '||p_instance_id;  
      MROI_TT_STATUS_UTIL_PKG.update_trx_status_batch(p_trx_status_tbl    => l_trx_valid_status_tbl
                                                     ,p_user_id           => p_user_id                                                               
                                                     ,p_x_mimes_err_code  => l_x_mimes_err_code
                                                     ,p_x_mimes_err_msg   => l_x_mimes_err_msg);
      IF l_x_mimes_err_code = 1 THEN                                            
             RAISE e_mimes_exception;                                             
      END IF;
      l_trx_valid_status_tbl.delete;
      l_t_ops_hdr_stg.delete;
      
      l_dbg_msg := '0014. Calling output_trans_status to write transaction status code to output file. ';
      output_trans_status(p_instance_id     => p_instance_id                      
                         ,p_x_mroi_err_code => p_x_mroi_err_code
                         ,p_x_mroi_err_msg  => p_x_mroi_err_msg); 
   EXCEPTION
      WHEN e_mimes_exception THEN
         p_x_mimes_err_code := l_x_mimes_err_code; 
         p_x_mimes_err_msg := 'Error occured when '||l_dbg_msg||' - '||l_x_mimes_err_msg; 
         FND_FILE.PUT_LINE(FND_FILE.LOG, p_x_mimes_err_msg);
      WHEN OTHERS THEN
         p_x_mroi_err_code := 1;
         p_x_mroi_err_msg := 'Error while '||l_dbg_msg||' - '||SQLERRM;
         FND_FILE.PUT_LINE(FND_FILE.LOG, p_x_mroi_err_msg);
   END validate_operations_records;

   /* *************************************************************************************
   Name: create_operations_records
   Object Type: Procedure
   Description:
   Input params: p_instance_id
                 p_x_mroi_err_code
                 p_x_mroi_err_msg

   Out params:

   Change History:
   Date         Name                      Ver    Modification
   -----------  ----------------------    ----   ------------
   05/15/2019   Ranadheer Reddy           1.0    Initial Creation
   ************************************************************************************* */
   PROCEDURE create_operations_records(
               p_instance_id       IN NUMBER
              ,p_user_id           IN NUMBER
              ,p_conv_mode         IN VARCHAR2
              ,p_t_ops_hdr_stg    IN t_ops_hdr_stg_tbl_type
              ,p_failed_api_count OUT NUMBER
              ,p_ahl_ops_count    OUT NUMBER
              ,p_x_mroi_err_code  OUT NUMBER
              ,p_x_mroi_err_msg   OUT VARCHAR2
              ,p_x_mimes_err_code OUT NUMBER
              ,p_x_mimes_err_msg  OUT VARCHAR2) IS

      c_api_version               NUMBER            := 2.0;
      c_init_msg_list             VARCHAR2(30)      := FND_API.G_TRUE;
      c_commit                    VARCHAR2(30)      := FND_API.G_FALSE;
      c_validation_level          NUMBER            := FND_API.G_VALID_LEVEL_FULL;
      c_default                   VARCHAR2(30)      := FND_API.G_FALSE;
      l_dbg_msg                   VARCHAR2(300);
      l_status_code               NUMBER;
      l_new_operation_id          NUMBER;
      l_return_status             VARCHAR2(20);
      l_msg_count                 NUMBER;
      l_msg_data                  VARCHAR2(4000);
      l_error_msg                 VARCHAR2(4000);
      l_status                    VARCHAR2(15);

      l_segment1                  VARCHAR2(30);
      l_segment2                  VARCHAR2(30);
      l_segment3                  VARCHAR2(30);
      l_segment4                  VARCHAR2(30);

      l_doc_count                 NUMBER := 0;
      l_res_count                 NUMBER := 0;
      l_itm_count                 NUMBER := 0;
      l_rec_count                 NUMBER := 0;
      l_x_ahl_operations          AHL_RM_OPERATION_PVT.operation_rec_type;
      l_x_ahl_doc                 AHL_RM_ASSO_DOCASO_PVT.doc_association_tbl;
      l_x_ahl_resource            AHL_RM_RT_OPER_RESOURCE_PVT.rt_oper_resource_tbl_type;
      l_x_ahl_material            AHL_RM_MATERIAL_AS_PVT.material_req_tbl_type;
      l_x_ahl_panel               AHL_RM_RT_OPER_PANEL_PVT.rt_oper_panel_tbl_type;
      l_start_Date                DATE;
      l_p_attribute1              VARCHAR2(4000);
      
      l_x_mimes_err_code NUMBER;
      l_x_mimes_err_msg  VARCHAR2(1000);

     
      /*CURSOR c_op_header IS
            SELECT * FROM xxmro.MROI_AHL_OPS_HDR_STG
            WHERE STATUS = 'Validated';*/

      CURSOR c_op_lines (p_record_header_id NUMBER) IS
            SELECT *
              FROM xxmro.mroi_ahl_ops_line_stg
             WHERE record_header_id = p_record_header_id
          ORDER BY record_line_id;
   BEGIN
      p_x_mroi_err_code := 0;
      p_failed_api_count := 0;
      p_ahl_ops_count := 0;
      FOR i IN p_t_ops_hdr_stg.FIRST..p_t_ops_hdr_stg.LAST LOOP
         l_error_msg := NULL;
         l_status := NULL;
         l_return_status := NULL;
         l_msg_count := NULL;
         l_msg_data  := NULL;
         l_rec_count := l_rec_count + 1;
         l_new_operation_id := NULL;
         l_doc_count := 0;
         l_res_count := 0;
         l_itm_count := 0;
         l_x_ahl_doc.delete;
         l_x_ahl_resource.delete;
         l_x_ahl_material.delete;
         l_x_ahl_panel.delete;
         
         l_dbg_msg  := '103.01 Parsing operation id. ';
         -- Parse resource to individual segments --
         l_segment1 := substr(p_t_ops_hdr_stg(i).operation_id,1,instr(p_t_ops_hdr_stg(i).operation_id,'.',1)-1);
         l_segment2 := substr(p_t_ops_hdr_stg(i).operation_id,instr(p_t_ops_hdr_stg(i).operation_id,'.',1)+1,instr(p_t_ops_hdr_stg(i).operation_id,'.',1,2)-instr(p_t_ops_hdr_stg(i).operation_id,'.',1)-1);
         l_segment3 := substr(p_t_ops_hdr_stg(i).operation_id,instr(p_t_ops_hdr_stg(i).operation_id,'.',1,2)+1,instr(p_t_ops_hdr_stg(i).operation_id,'.',1,3)-instr(p_t_ops_hdr_stg(i).operation_id,'.',1,2)-1);
         l_segment4 := substr(p_t_ops_hdr_stg(i).operation_id,instr(p_t_ops_hdr_stg(i).operation_id,'.',1,3)+1);

         l_dbg_msg  := '103.02 Assigning operation header parameters. ';
         -- Assign operation parameter values --
         l_x_ahl_operations.object_version_number := 1;
         l_x_ahl_operations.revision_number := 1;
         l_x_ahl_operations.segment1 := l_segment1;
         l_x_ahl_operations.segment2 := l_segment2;
         l_x_ahl_operations.segment3 := l_segment3;
         l_x_ahl_operations.segment4 := l_segment4;
         l_x_ahl_operations.attribute1 := p_t_ops_hdr_stg(i).competency_name;
         l_x_ahl_operations.attribute2 := p_t_ops_hdr_stg(i).occurrence_factor;
         l_x_ahl_operations.attribute3 := p_t_ops_hdr_stg(i).secondary_buyoff;
         l_x_ahl_operations.description := p_t_ops_hdr_stg(i).description;
         l_x_ahl_operations.operation_type := p_t_ops_hdr_stg(i).operation_type;
         l_x_ahl_operations.standard_operation_flag := (CASE when upper(p_t_ops_hdr_stg(i).operation_type) = 'YES' THEN 'Y' ELSE 'N' END);
         BEGIN 
              l_start_date := to_date (p_t_ops_hdr_stg(i).start_date, 'MM/DD/YYYY');
              IF l_Start_Date < sysdate THEN
                   l_start_date := sysdate;
              END IF;
         EXCEPTION 
              WHEN OTHERS THEN
                   l_start_date := sysdate;
         END;    
         l_x_ahl_operations.active_start_date := l_start_Date;
         l_x_ahl_operations.qa_inspection_type_desc := p_t_ops_hdr_stg(i).quality_inspection_type;
         l_x_ahl_operations.dml_operation := 'C';
         
         FOR r_op_lines IN c_op_lines(p_t_ops_hdr_stg(i).record_header_id) LOOP

            -- Assign document parameter values --
            l_dbg_msg  := '103.03 Assigning parameter values for document. ';
            IF r_op_lines.rd_document_num IS NOT NULL THEN
               l_doc_count := l_doc_count + 1;
               l_x_ahl_doc(l_doc_count).document_no := r_op_lines.rd_document_num;
               l_x_ahl_doc(l_doc_count).document_id := r_op_lines.rd_document_id;
               l_x_ahl_doc(l_doc_count).revision_no := 1;
            END IF;

            -- Assign resource parameter values --
            l_dbg_msg  := '103.04 Assigning parameter values for resource. ';
            IF r_op_lines.rr_resource IS NOT NULL THEN
               l_res_count := l_res_count + 1;
            -- l_x_ahl_resource(l_res_count).aso_resource_name := r_op_lines.rr_resource;
               l_x_ahl_resource(l_res_count).resource_type :=  r_op_lines.rr_resource_type;
               l_x_ahl_resource(l_res_count).aso_resource_id := r_op_lines.rr_resource_id;
               l_x_ahl_resource(l_res_count).quantity := r_op_lines.rr_quantity;
               l_x_ahl_resource(l_res_count).duration := r_op_lines.rr_duration;
               l_x_ahl_resource(l_res_count).dml_operation := 'C';
               l_x_ahl_resource(l_res_count).schedule_seq := r_op_lines.rr_schedule_sequence;
            END IF;

            -- Assign material/item parameter values --
            l_dbg_msg  := '103.05 Assigning parameter values for item. ';
            IF r_op_lines.mr_item IS NOT NULL THEN
               l_itm_count := l_itm_count + 1;
               l_x_ahl_material(l_itm_count).object_version_number := 1;
       --      l_x_ahl_material(l_itm_count).item_number := r_op_lines.mr_item;       
               l_x_ahl_material(l_itm_count).inventory_org_id := mroi_util_pkg.get_master_org_id;
               l_x_ahl_material(l_itm_count).inventory_item_id := r_op_lines.mr_item_id;
               l_x_ahl_material(l_itm_count).quantity := r_op_lines.mr_quantity;
               l_x_ahl_material(l_itm_count).uom := 'Each';
               l_x_ahl_material(l_itm_count).dml_operation := 'C';
            END IF;
         END LOOP;

           
         -- Call API to create operation --
         SAVEPOINT resc;
         l_dbg_msg  := '103.06 Calling Create_Operation API. ';
         AHL_RM_OPERATION_PUB.Create_Operation(
               p_api_version         => c_api_version,
               p_init_msg_list       => c_init_msg_list,
               p_commit              => c_commit,
               p_validation_level    => c_validation_level,
               p_default             => c_default,
               p_module_type         => NULL,
               x_return_status       => l_return_status,
               x_msg_count           => l_msg_count,
               x_msg_data            => l_msg_data,
               p_x_oper_rec          => l_x_ahl_operations,
               p_x_oper_doc_tbl      => l_x_ahl_doc,
               p_x_oper_resource_tbl => l_x_ahl_resource,
               p_x_oper_material_tbl => l_x_ahl_material,
               p_x_oper_panel_tbl    => l_x_ahl_panel);

         IF nvl(l_return_status,'x') <> 'S' THEN
            ROLLBACK to resc;
            p_failed_api_count := p_failed_api_count + 1;
            l_status := 'Error-API';
            l_p_attribute1 := 'AHL Operation creation API call';
            FOR i IN 1..fnd_msg_pub.count_msg LOOP
               IF l_error_msg IS NULL THEN
                  l_error_msg := fnd_msg_pub.get(p_msg_index => i, p_encoded => 'F' );
               ELSE
                  l_error_msg := l_error_msg||' '||fnd_msg_pub.get(p_msg_index => i, p_encoded => 'F' );
               END IF;
            END LOOP;
            fnd_file.put_line(fnd_file.log,'l_error_msg: ' || l_error_msg);
         ELSE
            -- Initialize approval --
            l_dbg_msg  := '103.07 Calling Initiate_Oper_Approval API. ';
            ahl_rm_operation_pub.Initiate_Oper_Approval(
                  p_api_version           => 1.0,
                  p_init_msg_list         => c_init_msg_list,
                  p_commit                => c_commit,
                  p_validation_level      => c_validation_level,
                  p_default               => c_default,
                  p_module_type           => NULL,
                  x_return_status         => l_return_status,
                  x_msg_count             => l_msg_count,
                  x_msg_data              => l_msg_data,
                  p_oper_id               => l_x_ahl_operations.operation_id,
                  p_oper_number           => null,
                  p_oper_revision         => 1,
                  p_oper_object_version   => 1,
                  p_apprv_type            =>'APPROVE'); 
            --l_return_status := 'S'; 
            IF nvl(l_return_status,'x') <> 'S' THEN
               ROLLBACK to resc;
               p_failed_api_count := p_failed_api_count + 1;
               l_status := 'Error-API';
               l_p_attribute1 := 'AHL Operation Approval API call';
               FOR i IN 1..fnd_msg_pub.count_msg LOOP
                  IF l_error_msg IS NULL THEN
                     l_error_msg := fnd_msg_pub.get(p_msg_index => i, p_encoded => 'F' );
                  ELSE
                     l_error_msg := l_error_msg||' '||fnd_msg_pub.get(p_msg_index => i, p_encoded => 'F' );
                  END IF;
               END LOOP;
            ELSE
               BEGIN
                  l_dbg_msg  := '103.08 Checking operation created in EBS. ';
                  SELECT operation_id
                    INTO l_new_operation_id
                    FROM ahl_operations_b_kfv
                   WHERE concatenated_segments = p_t_ops_hdr_stg(i).operation_id;
                  l_status := 'Loaded-EBS';
                  p_ahl_ops_count := p_ahl_ops_count + 1;
               EXCEPTION
                 WHEN NO_DATA_FOUND THEN
                     l_status := 'Missing-EBS';
                     l_error_msg := 'Cannot find operation in EBS after creation';
               END;

               IF p_conv_mode = 'V' THEN
                  ROLLBACK to resc;
               END IF;
            END IF;
         END IF;

         IF l_status = 'Loaded-EBS' THEN
            l_status_code := MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_LOADED_EBS;
         ELSIF l_status = 'Missing-EBS' THEN
            l_status_code := MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_MISSING_EBS;
         ELSE
            l_status_code := MROI_TT_STATUS_UTIL_PKG.G_TRXCODE_ERRED_OIT_API;
         END IF;

         l_dbg_msg  := '103.09 Updating status in staging table. ';
         UPDATE xxmro.mroi_ahl_ops_hdr_stg
            SET status = l_status,
                error_message = l_error_msg
          WHERE record_header_id=p_t_ops_hdr_stg(i).record_header_id;

         IF l_error_msg IS NOT NULL THEN
            l_dbg_msg  := '103.10 Inserting error in the error table. ';
            mroi_tt_error_util_pkg.log_error_single(
                        p_instance_id        => p_instance_id
                       ,p_transaction_id     => p_t_ops_hdr_stg(i).transaction_id
                       ,p_record_level_id    => p_t_ops_hdr_stg(i).record_header_id
                       ,p_record_level_type  => 'OIT_API'
                       ,p_error_code         => 16000
                       ,p_error_field        => 'OPERATION_ID'
                       ,p_error_attribute1 => l_p_attribute1 
                       ,p_error_attribute2 => l_error_msg
                       ,p_error_attribute3 => null
                       ,p_error_attribute4 => null
                       ,p_ora_err_code       => null
                       ,p_ora_err_msg        => null
                       ,p_user_id            => p_user_id
                       ,p_x_mimes_err_code   => l_x_mimes_err_code
                       ,p_x_mimes_err_msg    => l_x_mimes_err_msg);
            p_x_mimes_err_code := l_x_mimes_err_code;
            p_x_mimes_err_msg := l_x_mimes_err_msg;

            IF l_x_mimes_err_code = 1 THEN
               return;
            END IF;         
         END IF;

         l_dbg_msg  := '103.11 Calling mroi_tt_status_util_pkg.update_trx_status_single. ';
         mroi_tt_status_util_pkg.update_trx_status_single(
                                      p_transaction_id => p_t_ops_hdr_stg(i).transaction_id,
                                      p_status_Code => l_status_code,
                                      p_reprocess_instance_id => null,
                                      p_reject_reason => null,
                                      p_remarks => null,
                                      p_oit_reject_reason => l_error_msg,
                                      p_oit_remarks => null,
                                      p_user_id => p_user_id,
                                      p_x_mimes_err_code => l_x_mimes_err_code,
                                      p_x_mimes_err_msg => l_x_mimes_err_msg);

         p_x_mimes_err_code := l_x_mimes_err_code;
         p_x_mimes_err_msg := l_x_mimes_err_msg;

         IF l_x_mimes_err_code = 1 THEN
            return;
         END IF;

         IF mod(l_rec_count,500) = 0 THEN
            commit;
         END IF;
      END LOOP;
      COMMIT;
   EXCEPTION
      WHEN OTHERS THEN
         p_x_mroi_err_code := 1;
         p_x_mroi_err_msg := 'Unexpected error while in MROI_AHL_OPS_CONV_PKG.create_operations_records '||l_dbg_msg||' - '||SQLERRM;
         fnd_file.put_line(fnd_file.log, p_x_mroi_err_msg);
   END create_operations_records;

  /* *************************************************************************************
   Name: write_transaction_status
   Object Type: Procedure
   Description:
   Input params: p_x_mroi_err_code
                 p_x_mroi_err_msg

   Out params:

   Change History:
   Date         Name                      Ver    Modification
   -----------  ----------------------    ----   ------------
   05/15/2019   Ranadheer Reddy           1.0    Initial Creation
   ************************************************************************************* */
   PROCEDURE write_transaction_status(p_instance_id IN NUMBER
                                    ,p_x_mroi_err_code OUT NUMBER
                                    ,p_x_mroi_err_msg  OUT VARCHAR2) IS

      CURSOR c_hdr_validation_err (v_instance_id IN NUMBER) IS
         SELECT a.error_id,
                a.error_field,
                a.error_code,
                a.transaction_id, 
                b.operation_id, 
                a.record_level_id, 
                a.record_level_type, 
                MROI_TT_ERROR_UTIL_PKG.GET_ERROR_MSG(a.ERROR_CODE,'',a.ERROR_FIELD,a.ERROR_ATTRIBUTE1, 
                                                       a.ERROR_ATTRIBUTE2, a.ERROR_ATTRIBUTE3, 
                                                       a.ERROR_ATTRIBUTE4, a.ORA_ERR_CODE,a.ORA_ERR_MSG) error_message
           FROM mroi_ahl_ops_err a, 
                mroi_ahl_ops_hdr_stg b
           WHERE a.transaction_id = b.transaction_id 
             and a.record_level_type in ('HEADER', 'LINE')
             and b.instance_id = v_instance_id;
      
      CURSOR c_hdr_api_err (v_instance_id IN NUMBER) IS
         SELECT a.error_id,
                a.error_field,
                a.error_code,
                a.transaction_id, 
                b.operation_id, 
                a.record_level_id, 
                a.record_level_type, 
                MROI_TT_ERROR_UTIL_PKG.GET_ERROR_MSG(a.ERROR_CODE,'',a.ERROR_FIELD,a.ERROR_ATTRIBUTE1, 
                                                       a.ERROR_ATTRIBUTE2, a.ERROR_ATTRIBUTE3, 
                                                       a.ERROR_ATTRIBUTE4, a.ORA_ERR_CODE,a.ORA_ERR_MSG) error_message
           FROM mroi_ahl_ops_err a, 
                mroi_ahl_ops_hdr_stg b
           WHERE a.transaction_id = b.transaction_id 
             and a.record_level_type = 'OIT_API'
             and b.instance_id = v_instance_id;       

   BEGIN
      p_x_mroi_err_code := 0;
      -- Write headings --
      fnd_file.put_line(fnd_file.output,'');
      fnd_file.put_line(fnd_file.output,'******************************Records Failed during Validation***********************************************');
      fnd_file.put_line(fnd_file.output,
               rpad('-',20,'-')||' '||
               rpad('-',20,'-')||' '||
               rpad('-',60,'-')||' '||
               rpad('-',20,'-')||' '||
               rpad('-',30,'-')||' '||
               rpad('-',20,'-')||' '||
               rpad('-',40,'-'));

      fnd_file.put_line(fnd_file.output,
               rpad('Record Level Id', 20, ' ') || ' ' ||
               rpad('Transaction Id', 20, ' ') || ' ' ||
               rpad('Operation Id', 60, ' ') || ' ' ||
               rpad('Record Level Type', 20, ' ') || ' ' ||
               rpad('Error Field',30,' ')     ||' '||
               rpad('Error Code',20,' ')      ||' '||
               rpad('Error Message',40,' '));
          
      fnd_file.put_line(fnd_file.output,
               rpad('-',20,'-')||' '||
               rpad('-',20,'-')||' '||
               rpad('-',60,'-')||' '||
               rpad('-',20,'-')||' '||
               rpad('-',30,'-')||' '||
               rpad('-',20,'-')||' '||
               rpad('-',40,'-'));

      FOR l_hdr IN c_hdr_validation_err (p_instance_id) LOOP
         -- Write header level error
         fnd_file.put_line(fnd_file.output,
               rpad(l_hdr.Record_level_id,20,' ')     ||' '||
               rpad(l_hdr.transaction_id,20,' ')     ||' '||
               rpad(l_hdr.operation_id,60,' ')     ||' '||
               rpad(l_hdr.record_level_type,20,' ')     ||' '||
               rpad(l_hdr.error_field,30,' ')     ||' '||
               rpad(l_hdr.error_code,20,' ')      ||' '||              
               l_hdr.error_message);
      END LOOP;
      
      fnd_file.put_line(fnd_file.output,'');
      fnd_file.put_line(fnd_file.output,'******************************Records Failed during API******************************************************');
      fnd_file.put_line(fnd_file.output,
               rpad('-',20,'-')||' '||
               rpad('-',20,'-')||' '||
               rpad('-',60,'-')||' '||
               rpad('-',20,'-')||' '||
               rpad('-',30,'-')||' '||
               rpad('-',20,'-')||' '||
               rpad('-',40,'-'));

      fnd_file.put_line(fnd_file.output,
               rpad('Record Level Id', 20, ' ') || ' ' ||
               rpad('Transaction Id', 20, ' ') || ' ' ||
               rpad('Operation Id', 60, ' ') || ' ' ||
               rpad('Record Level Type', 20, ' ') || ' ' ||
               rpad('Error Field',30,' ')     ||' '||
               rpad('Error Code',20,' ')      ||' '||
               rpad('Error Message',40,' '));
          
      fnd_file.put_line(fnd_file.output,
               rpad('-',20,'-')||' '||
               rpad('-',20,'-')||' '||
               rpad('-',60,'-')||' '||
               rpad('-',20,'-')||' '||
               rpad('-',30,'-')||' '||
               rpad('-',20,'-')||' '||
               rpad('-',40,'-'));
       FOR l_hdr IN c_hdr_api_err (p_instance_id) LOOP
         -- Write header level error
         fnd_file.put_line(fnd_file.output,
               rpad(l_hdr.Record_level_id,20,' ')     ||' '||
               rpad(l_hdr.transaction_id,20,' ')     ||' '||
               rpad(l_hdr.operation_id,60,' ')     ||' '||
               rpad(l_hdr.record_level_type,20,' ')     ||' '||
               rpad(l_hdr.error_field,30,' ')     ||' '||
               rpad(l_hdr.error_code,20,' ')      ||' '||              
               l_hdr.error_message);
      END LOOP;
   EXCEPTION
      WHEN OTHERS THEN
         p_x_mroi_err_code := 1;
         p_x_mroi_err_msg := 'Error while '||' - '||SQLERRM;
   END write_transaction_status;
END mroi_ahl_ops_conv_pkg;