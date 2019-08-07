create or replace PACKAGE      MROI_HR_LOCATION_CONV_PKG AS
 /* ***************************************************************************
 /*  Name:          MROI_HR_LOCATION_CONV_PKG 
 /*  Object Type:   Package Specs
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
 

PROCEDURE PROCESS_HR_LOCATION(
                                p_x_errbuf OUT VARCHAR2,
                                p_x_retcode OUT NUMBER,
                                P_MODE IN VARCHAR2,
                                P_FILE_NAME IN VARCHAR2,
                                P_INSTANCE_ID IN NUMBER,
                                P_MIN_INDEX IN NUMBER,
                                P_MAX_INDEX IN NUMBER
                            );
                                                           
END MROI_HR_LOCATION_CONV_PKG;
