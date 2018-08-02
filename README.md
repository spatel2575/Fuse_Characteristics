# Fuse_Characteristics
Python code to generate variables for FUSE model

The following variables are generated for the FUSE Model:
 - DAYS_TO_FTRAN
 - FWPCTPRCHCL
 - FWAMTPRCH
 - FWNUMTHIGH
 - FWNUMCASH
 - FWMCCNUMTH
 
 Input data : Redshift Server / Creditshop / dpt_daily_transactions
 Output : Redshift Server 
 
 There are 2 python codes for the same
  - Fuse_charac_hist.py : This code creates variables for all accounts for all their past transactions.
  - Fuse_charac.py : This code creates variables for only those accounts whose 7th transaction day = todays.date - 1. Once the 
  variables have been created on historic data, this code needs to be setup to run daily.
 
