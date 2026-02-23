-- back compat for old kwarg name
  
  begin;
    
        
            
	    
	    
            
        
    

    

    merge into MY_DB.MY_SCHEMA.dim_products as DBT_INTERNAL_DEST
        using MY_DB.MY_SCHEMA.dim_products__dbt_tmp as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.productid = DBT_INTERNAL_DEST.productid))

    
    when matched then update set
        "PRODUCTID" = DBT_INTERNAL_SOURCE."PRODUCTID","NAME" = DBT_INTERNAL_SOURCE."NAME","SKU" = DBT_INTERNAL_SOURCE."SKU","L1" = DBT_INTERNAL_SOURCE."L1","L2" = DBT_INTERNAL_SOURCE."L2","WARRANTYMONTHS" = DBT_INTERNAL_SOURCE."WARRANTYMONTHS","COLOR" = DBT_INTERNAL_SOURCE."COLOR","ACTIVE" = DBT_INTERNAL_SOURCE."ACTIVE","UPDATED_AT" = DBT_INTERNAL_SOURCE."UPDATED_AT"
    

    when not matched then insert
        ("PRODUCTID", "NAME", "SKU", "L1", "L2", "WARRANTYMONTHS", "COLOR", "ACTIVE", "UPDATED_AT")
    values
        ("PRODUCTID", "NAME", "SKU", "L1", "L2", "WARRANTYMONTHS", "COLOR", "ACTIVE", "UPDATED_AT")

;
    commit;