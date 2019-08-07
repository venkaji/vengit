create or replace PACKAGE MROI_AHL_ITEM_GROUP_CONV_PKG AS
 /* ***************************************************************************
 /*  Name:          MROI_AHL_ITEM_GROUP_CONV_PKG 
 /*  Object Type:   Package Specification
 /*  Description:   Package Specification for Item Group Conversion
 /*
 /*  RICE Type:     Conversion
 /*  RICE ID:    	0007A
 /*
 /*  Change History:
 /*  Date          Name                           Ver      Modification
 /*  -----------   ----------------------         ----     -------------
 /*  06/05/2019    Sarah Gadzinski                1.0      Initial Creation
 /******************************************************************************/
 
-- Main conversion procedure invoked by a concurrent program
PROCEDURE main (
				p_x_errbuf OUT VARCHAR2,
				p_x_retcode OUT NUMBER,
				p_mode IN VARCHAR2,
				p_filename IN VARCHAR2
				);
				
-- Procedure to read from external table and load into staging table, then invoke MIMES
PROCEDURE stage_file_records (
				p_instance_id IN NUMBER,
				p_user_id IN NUMBER,
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

-- Procedure to write to the concurrent request log
PROCEDURE write_log(p_string VARCHAR2);

-- Procedure to write to the concurrent request output
PROCEDURE write_output(p_string VARCHAR2);

END MROI_AHL_ITEM_GROUP_CONV_PKG;