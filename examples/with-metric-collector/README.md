If you run this example and go to http://localhost:8000/metrics, 

you can see first and second consumet metrics as shown below

```
# HELP first_discarded_messages_total_current Total number of discarded messages.
# TYPE first_discarded_messages_total_current counter
first_discarded_messages_total_current 0

# HELP first_processed_messages_total_current Total number of processed messages.
# TYPE first_processed_messages_total_current counter
first_processed_messages_total_current 0

# HELP first_retried_messages_total_current Total number of retried messages.
# TYPE first_retried_messages_total_current counter
first_retried_messages_total_current 0

# HELP first_unprocessed_messages_total_current Total number of unprocessed messages.
# TYPE first_unprocessed_messages_total_current counter
first_unprocessed_messages_total_current 0

# HELP second_discarded_messages_total_current Total number of discarded messages.
# TYPE second_discarded_messages_total_current counter
second_discarded_messages_total_current 0

# HELP second_processed_messages_total_current Total number of processed messages.
# TYPE second_processed_messages_total_current counter
second_processed_messages_total_current 0

# HELP second_retried_messages_total_current Total number of retried messages.
# TYPE second_retried_messages_total_current counter
second_retried_messages_total_current 0

# HELP second_unprocessed_messages_total_current Total number of unprocessed messages.
# TYPE second_unprocessed_messages_total_current counter
second_unprocessed_messages_total_current 0
```