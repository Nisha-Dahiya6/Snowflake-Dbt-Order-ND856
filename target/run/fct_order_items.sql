-- back compat for old kwarg name
  
  begin;
    
        
            
                
                
            
                
                
            
        
    

    

    merge into MY_DB.MY_SCHEMA.fct_order_items as DBT_INTERNAL_DEST
        using MY_DB.MY_SCHEMA.fct_order_items__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.order_id = DBT_INTERNAL_DEST.order_id
                ) and (
                    DBT_INTERNAL_SOURCE.item_id = DBT_INTERNAL_DEST.item_id
                )

    
    when matched then update set
        "ORDER_ID" = DBT_INTERNAL_SOURCE."ORDER_ID","ITEM_ID" = DBT_INTERNAL_SOURCE."ITEM_ID","PRODUCT_ID" = DBT_INTERNAL_SOURCE."PRODUCT_ID","QTY" = DBT_INTERNAL_SOURCE."QTY","UNITPRICE" = DBT_INTERNAL_SOURCE."UNITPRICE","CURRENCY" = DBT_INTERNAL_SOURCE."CURRENCY","GROSS_AMOUNT" = DBT_INTERNAL_SOURCE."GROSS_AMOUNT","DISCOUNT_AMOUNT" = DBT_INTERNAL_SOURCE."DISCOUNT_AMOUNT","NET_AMOUNT" = DBT_INTERNAL_SOURCE."NET_AMOUNT","ORDER_UPDATED_AT" = DBT_INTERNAL_SOURCE."ORDER_UPDATED_AT"
    

    when not matched then insert
        ("ORDER_ID", "ITEM_ID", "PRODUCT_ID", "QTY", "UNITPRICE", "CURRENCY", "GROSS_AMOUNT", "DISCOUNT_AMOUNT", "NET_AMOUNT", "ORDER_UPDATED_AT")
    values
        ("ORDER_ID", "ITEM_ID", "PRODUCT_ID", "QTY", "UNITPRICE", "CURRENCY", "GROSS_AMOUNT", "DISCOUNT_AMOUNT", "NET_AMOUNT", "ORDER_UPDATED_AT")

;
    commit;