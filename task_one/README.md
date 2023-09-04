# Task One: Building an ETL Pipeline

## The Scenario
You've been working with a team of Applied Data Scientists on the task of identifying customers and invoices that are at risk of late payments. As a team you've made good progress, and the scientists are now confident that they can build a useful model. You're now tasked with writing the ETL jobs to create the features (see below) that the scientists need to train and test models.

The `data_extracts` folder contains csv extracts from 4 production tables:

### accounts
Contains general account information about our customers

### skus
Contains information about the products (or Stock Keeping Units - SKUs) we sell

### invoices
Contains information about invoices issued to customers

### invoice_line_items
Contains detailed information about the products provided on each invoice

## The Features (i.e. The dataset to be constructed)
After much exploration and testing, the scientists in your team have settled no the following features to train their model. Their model will take as an input information about a single invoice, and return the probability of the invoice being paid late. They've asked that your ETL pipeline produce a single dataset, with each row representing a single invoice, and containing the following columns:

- **inv_id**: the ID of the invoice to be passed into the model
- **acct_id**: the ID of the account for that invoice
- **inv_total**: Total value (in dollars) for the invoice
- **inv_items**: Total number of items in the invoice
- **acct_age**: The age of the account (in days) at the time the invoice was issued (i.e. the number of days between when the account was set up, and the invoice date)
- **num_inv_120d**: the number of invoices for the account in the 120 days prior to the invoice's issuing date
- **cum_tot_inv_acct**: The cumulative number of invoices for the account up to date that the invoice was issued.
- **is_late**: A flag to indicate if the invoice was paid late (i.e. more than 30 days after issuing). Contains `1` if invoice was paid late, `0` otherwise.

## Your Submission
Add to this repository the code and documentation that:
- Uses Spark (or a framework you are comfortable in) to transform the raw data into the features required by the scientists
- Tests your ETL job using the CSV extracts provided
- Highlights any intermediate data models you might use to store the data and why
- Highlights any design choices you've made, as well as scaling considerations
