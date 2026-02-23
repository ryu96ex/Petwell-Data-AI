The project consists of five main components:

1. Service API:
   Responsible for file upload handling and verfication and retrieving pet health data from our google cloud database instance to be displayed on graphs.
   
2. Service Worker:
   Subscribed to pub sub event for file upload. Once event notification is received from subscription, Responsible for utilizing OCR to convert unreadable PDF
   files to raw text and AI text extraction into structured JSON format to be inserted into database instance.

3. Database Instance
   Holds data for pet health metrics, file upload records, user and pet info.

4. Cloud Storage Bucket
   Storage to hold uploaded DF files as BLOBs.

5. Pub Sub
   Event manager to decouple service components.
