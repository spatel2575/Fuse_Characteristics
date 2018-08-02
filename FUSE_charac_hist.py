import pandas as pd
import datetime
import csv
import psycopg2
from encrypt import decryption

connection = psycopg2.connect(
            dbname = decryption('9Qi/0hOCgRf7vEJ5LdJipfUIv9ITgoEX+7xCeS3SYqX1CL/SE4KBF/u8Qnkt0mKlAW73XZWsVR4TShKGuuyEIg=='),
            host = decryption('9Qi/0hOCgRf7vEJ5LdJipa8F56V3H8+arDxCh33yXk2ZZ30mHkix7FPXAH0eZq4vNcxFp7ORpOpdaWgfDFEyBw=='),
            port = decryption('9Qi/0hOCgRf7vEJ5LdJipfUIv9ITgoEX+7xCeS3SYqX1CL/SE4KBF/u8Qnkt0mKlYBecqcPx27gKVllud8l+0A=='),
            user = decryption('9Qi/0hOCgRf7vEJ5LdJipfUIv9ITgoEX+7xCeS3SYqX1CL/SE4KBF/u8Qnkt0mKl6KBUo5Y9Em0HA7p5ctNZtg=='),
            password = decryption('9Qi/0hOCgRf7vEJ5LdJipfUIv9ITgoEX+7xCeS3SYqX1CL/SE4KBF/u8Qnkt0mKlQGwfChN6W1sLm0McZe1QoQ==')
            )
cursor = connection.cursor()


test_query = 'select a.account_id, a.transaction_date, a.transaction_amount, b.date_open, b.original_credit_limit, a.output_transaction_code_internal, a.transaction_category, a.terms_balance, a.merchant_sic_class_code, a.debit_credit_indicator from dpt_daily_transactions a left join cif_account_data b on a.account_id = b.account_id where a.account_id < 200'
cursor.execute(test_query)
data = cursor.fetchall()

AFF_dataframe = pd.read_csv('AFF_lookup.csv')
input_dataframe = pd.read_csv('QA_check.csv')
input_dataframe = input_dataframe.drop(['a'], axis=1)
input_dataframe = pd.DataFrame(data, columns=['account_id', 'transaction_date', 'transaction_amount', 'date_open', 'original_credit_limit', 'tran_code', 'tran_cat_code', 'tran_terms_bal', 'merchant_category_code','debit_credit_indicator'])
input_dataframe = pd.merge(input_dataframe, AFF_dataframe, how='left',left_on = ['tran_code', 'tran_cat_code', 'tran_terms_bal'], right_on = ['tran_code', 'tran_cat_code', 'tran_terms_bal'])


column_name_list = list(input_dataframe)
for i in range(0,len(column_name_list)):
    if 'Date' in column_name_list[i] or 'date' in column_name_list[i]:
        input_dataframe[column_name_list[i]] = pd.to_datetime(input_dataframe[column_name_list[i]]).dt.date

counter = 0

final_dataframe = pd.DataFrame(columns=['Account_ID', '7th_TRAN_DATE', 'DAYS_TO_FTRAN', 'FWAMTPRCH', 'FWPCTPRCHCL', 'FWMCCNUMTH', 'FWNUMCASH', 'FWNUMTHIGH'])



for acct_id in input_dataframe['account_id'].unique():

    high_count = 0
    low_count = 0
    debit = 0
    credit = 0
    DAYS_TO_FTRAN = 0
    FWPCTPRCHCL	= 0
    FWAMTPRCH = 0
    FWMCCNUMTH = 0	
    FWNUMCASH = 0	
    FWNUMTHIGH = 0

    acc_df = input_dataframe.loc[input_dataframe['account_id'] == acct_id]

    aff_group_df = acc_df.loc[acc_df['Tran_Group'].isin(['Balance Transfer Amount','Cash Advance Amount','Convenience Check Amount','Purchases'])]
    

    if not aff_group_df['account_id'].dropna().empty:

        first_transaction_date = min(aff_group_df['transaction_date'])
        seventh_transaction_date = (first_transaction_date + pd.offsets.DateOffset(days=6)).date()

        days_to_ftran_df = aff_group_df.loc[((aff_group_df['transaction_date'] - aff_group_df['date_open']).dt.days) <= 90]

        if not days_to_ftran_df['account_id'].dropna().empty:
            days_to_ftran_df = days_to_ftran_df.loc[((days_to_ftran_df['transaction_date'] - days_to_ftran_df['date_open']).dt.days) >= 0 ]

            if not days_to_ftran_df['account_id'].dropna().empty:
                first_transaction_date = min(days_to_ftran_df['transaction_date'])
                date_open = min(days_to_ftran_df['date_open'])
                DAYS_TO_FTRAN = (first_transaction_date - date_open).days

        fwpctprchcl_df = aff_group_df.loc[((aff_group_df['transaction_date'] - first_transaction_date).dt.days) <= 6]
        
        if not fwpctprchcl_df['account_id'].dropna().empty:

            original_credit_limit = max(fwpctprchcl_df['original_credit_limit'])
            fwpctprchcl_df = fwpctprchcl_df.loc[fwpctprchcl_df['Tran_Group'] == 'Purchases']

            debit_df = fwpctprchcl_df.loc[fwpctprchcl_df['debit_credit_indicator'] == 'D']
            credit_df = fwpctprchcl_df.loc[fwpctprchcl_df['debit_credit_indicator'] == 'C']
            
            if not debit_df['account_id'].dropna().empty:
                debit = sum(debit_df['transaction_amount'])

            if not credit_df['account_id'].dropna().empty:
                credit = sum(credit_df['transaction_amount'])

            FWAMTPRCH = round(debit + credit,2)

            if original_credit_limit > 0:
                FWPCTPRCHCL = round(((FWAMTPRCH * 100)/original_credit_limit),2)

        fwnumthigh_df = aff_group_df.loc[((aff_group_df['transaction_date'] - first_transaction_date).dt.days) <= 6]
        parent_counter_df = fwnumthigh_df.loc[fwnumthigh_df['Tran_Group'] == 'Cash Advance Amount']

        child_counter_high = parent_counter_df.loc[parent_counter_df['debit_credit_indicator'] == 'D']
        child_counter_low = parent_counter_df.loc[parent_counter_df['debit_credit_indicator'] == 'C']

        if not child_counter_high['account_id'].dropna().empty:
            high_count = len(child_counter_high['account_id'])

        if not child_counter_low['account_id'].dropna().empty:
            low_count = len(child_counter_low['account_id'])

        FWNUMCASH = high_count - low_count

        fwmccnumth_df = fwnumthigh_df.loc[fwnumthigh_df['merchant_category_code'].isin([5331,5735,5541,5521,5933,5967,7311,6010,6011,6012,6051,7273,7394,5599,5931,4899,4900,7993,7994,4814,4816,5944,5993])]        
        fwmccnumth_df = fwmccnumth_df.loc[fwmccnumth_df['debit_credit_indicator'] == 'D']
        FWMCCNUMTH = len(fwmccnumth_df['account_id'])

        FWNUMTHIGH = FWNUMCASH + FWMCCNUMTH

        final_dataframe.loc[counter]=[acct_id, seventh_transaction_date, DAYS_TO_FTRAN, FWAMTPRCH, FWPCTPRCHCL, FWMCCNUMTH, FWNUMCASH, FWNUMTHIGH]
        counter = counter + 1



header_list = list(final_dataframe)

table_name = ''

insert_header = 'Insert into ' + table_name + '('
insert_value = ' Values('

for i in range(0,len(final_dataframe['account_id'])):

    sql_1st_string = ''
    sql_2nd_string = ''
        
    for j in range(0,len(header_list)):
            
        if j < len(header_list)-1:
            
            sql_1st_string = sql_1st_string + header_list[j] + ', '
            sql_2nd_string = sql_2nd_string + "'" + str(final_dataframe[header_list[j]][i]) + "', "
            
        else:
            sql_1st_string = sql_1st_string + header_list[j] + ')'
            sql_2nd_string = sql_2nd_string + "'" + str(final_dataframe[header_list[j]][i]) + "') "      

    final_insert_string = insert_header + sql_1st_string + insert_value + sql_2nd_string
    cursor.execute(final_insert_string)
    
connection.commit()


