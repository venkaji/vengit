create or replace PACKAGE MROI_FND_LOOKUP_CONV_PKG AS
 /* ***************************************************************************
 /*  Name:          MROI_FND_LOOKUP_CONV_PKG 
 /*  Object Type:   Package Specification
 /*  Description:   Package Specification for FND Lookup Conversion (MC Positions)
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
 
-- Main conversion procedure invoked by a concurrent program
PROCEDURE main (
				p_x_errbuf OUT VARCHAR2,
				p_x_retcode OUT NUMBER,
				p_mode IN VARCHAR2,
				p_filename IN VARCHAR2,
				p_lookup_type IN VARCHAR2,
				p_application IN VARCHAR2
				);
				
-- Procedure to read from external table and load into staging table, then invoke MIMES
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
				);

-- Procedure to validate records in the staging table and invoke MIMES
PROCEDURE validate_records (
				p_instance_id IN NUMBER,
				p_user_id IN NUMBER,
				p_x_error_count OUT NUMBER,
				p_x_mroi_error_code OUT NUMBER,
				p_x_mroi_error_msg OUT VARCHAR2,
				p_x_mimes_error_code OUT NUMBER,
				p_x_mimes_error_msg OUT VARCHAR2
				);
				
-- Procedure to invoke the API for validated records and invoke MIMES
PROCEDURE load_records (
				p_instance_id IN NUMBER,
				p_user_id IN NUMBER,
				p_api_mode IN VARCHAR2,
				p_x_api_error_count OUT NUMBER,
				p_x_mroi_error_code OUT NUMBER,
				p_x_mroi_error_msg OUT VARCHAR2,
				p_x_mimes_error_code OUT NUMBER,
				p_x_mimes_error_msg OUT VARCHAR2
				);

-- Procedure to write to the concurrent request log
PROCEDURE write_log(p_string VARCHAR2);

-- Procedure to write to the concurrent request output
PROCEDURE write_output(p_string VARCHAR2);

-- Procedure to generate the conversion error report
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
				p_x_mroi_error_msg OUT VARCHAR2);
END MROI_FND_LOOKUP_CONV_PKG;