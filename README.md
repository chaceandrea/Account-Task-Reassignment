# Reassigner

This script utilizes simple-salesforce to make queries on our Salesforce Org for information on accounts and tasks currently assigned to a specific owner. It creates a  pandas dataframe from this information and uses API to re-assign the accounts and tasks as evenly as possible amongst the specified team members.
