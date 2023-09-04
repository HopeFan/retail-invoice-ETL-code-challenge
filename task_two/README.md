# Task Two: System Design

## The Scenario
Now that you have built an ETL job, the team is looking to design the production system that will take the data from the source system (TRS) and provide scientists with up-to-date data on a regular basis.

Following a discussion with the architects and scientists, you have discovered the following:
- TRS currently stores its data in AWS Aurora MySQL databases
- The TRS team currently does not have CDC enabled on those databases, but plan to within the next 12 months. When this happens, the CDC events will be placed onto Kafka.
- The scientists would like to be able to see what their features looked like at different points in time (e.g. one month ago vs today). They appreciate that they will need to make some compromises on how much data we can store, and will take your advice re: keeping old versions of the features.
- The finance and BI teams are also interested in the invoice data, and would like to query it. They are proficient in SQL only.

## Your Submission
Add to this repository documentation that:
- Shows the architecture of a production system that could run your (and similar) ETL jobs on a regular basis, before exposing the resulting datasets to the various consumers.
- Indicates how your architecture would evolve to support near real-time updating of data as the TRS engineering team enables CDC.
- Highlights any key architectural decisions made, and explains them.