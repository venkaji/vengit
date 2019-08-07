create or replace PACKAGE mroi_ahl_ops_conv_pkg AS
 /* ***************************************************************************
 /*  Name:          MROI_AHL_OPSP_CONV_PKG 
 /*  Object Type:   Package Specification
 /*  Description:   Package Specification for AHL Operation Conversion
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
 
  --Can declare variable of external table as long as the external table is not dynamically created;
  TYPE t_external_table_type IS TABLE OF MROI_AHL_OPS_EXT%ROWTYPE;

  -- Operation table type variable for header staging record
  TYPE t_ops_hdr_stg_tbl_type IS TABLE OF mroi_ahl_ops_hdr_stg%ROWTYPE;
  -- Operation table type variable for lines staging record
  TYPE t_ops_ln_stg_tbl_type IS TABLE OF mroi_ahl_ops_line_stg%ROWTYPE;

  -- record type to get STG header ID 
  TYPE t_hdr_id_rec IS RECORD(RECORD_HEADER_ID NUMBER);
  -- hdr rec ID table type
  TYPE t_hdr_id_tbl IS TABLE OF t_hdr_id_rec;

  -- record type to get STG line ID 
  TYPE t_line_id_rec IS RECORD(RECORD_LINE_ID NUMBER);
  -- hdr rec ID table type
  TYPE t_line_id_tbl IS TABLE OF t_line_id_rec;  

PROCEDURE process_operations_main(p_x_errbuf              OUT VARCHAR2
                         ,p_x_retcode             OUT NUMBER
						 ,p_conv_mode IN VARCHAR2); 

PROCEDURE stage_ahl_operations_records( p_instance_id IN NUMBER
                                       ,p_user_id IN NUMBER
                                       ,p_file_name IN VARCHAR2
                                       ,p_tech_id IN NUMBER
                                       ,p_x_total_trx_count OUT NUMBER
                                       ,p_x_mroi_err_code OUT NUMBER
                                       ,p_x_mroi_err_msg OUT VARCHAR2
                                       ,p_x_mimes_err_code OUT NUMBER
                                       ,p_x_mimes_err_msg OUT VARCHAR2);

PROCEDURE validate_operations_records( p_instance_id       IN NUMBER
                                      ,p_user_id           IN NUMBER
                                      ,p_error_count      OUT NUMBER
                                      ,p_t_ops_hdr_stg    OUT t_ops_hdr_stg_tbl_type
                                      ,p_x_mroi_err_code  OUT NUMBER
                                      ,p_x_mroi_err_msg   OUT VARCHAR2
                                      ,p_x_mimes_err_code OUT NUMBER
                                      ,p_x_mimes_err_msg  OUT VARCHAR2);


PROCEDURE create_operations_records( p_instance_id IN NUMBER
                                    ,p_user_id     IN NUMBER
                                    ,p_conv_mode   IN VARCHAR2
									,p_t_ops_hdr_stg    IN t_ops_hdr_stg_tbl_type
                                    ,p_failed_api_count OUT NUMBER
                                    ,p_ahl_ops_count    OUT NUMBER
                                    ,p_x_mroi_err_code  OUT NUMBER
                                    ,p_x_mroi_err_msg   OUT VARCHAR2
                                    ,p_x_mimes_err_code OUT NUMBER
                                    ,p_x_mimes_err_msg  OUT VARCHAR2);
                                    
PROCEDURE write_transaction_status( p_instance_id     IN NUMBER
                                   ,p_x_mroi_err_code OUT NUMBER
                                   ,p_x_mroi_err_msg  OUT VARCHAR2);

PROCEDURE conversion_report (p_start_date IN Date, 
                             p_end_date IN date, 
                             p_file_name IN VARCHAR2, 
                             p_total_record IN NUMBER, 
                             p_total_success_record IN NUMBER, 
                             p_total_failed_val_record IN NUMBER, 
                             p_total_failed_api_record IN NUMBER, 
                             p_total_ops_record IN NUMBER, 
                             p_conv_mode IN VARCHAR2);
                             
FUNCTION is_numeric (p_value IN VARCHAR2) RETURN NUMBER; 

END mroi_ahl_ops_conv_pkg;