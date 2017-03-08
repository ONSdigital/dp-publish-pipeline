## Dp Publishing Pipeline tests

### Setup
The following document describes the deployment of the new [pipeline](Deployment.md)
running on AWS. This is integrated into the existing infrastructure.

### Results from March 2017

| size          | Collection name                     | zebedee (publish / index) | dp (publish / index) |
|---------------|-------------------------------------|---------------------------|----------------------|
| 2k (133.4 MB) | 2016-12-23-09-31-qnajulytosept2016  | 2s / 23s                  |  1s / 3s             |
| 9k (424.8 MB) | 2016-12-23-09-32-ukeconomicaccounts | 8s / 118s                 |  8s / 13s            |

#### Notes
The biggest bottle neck in the pipeline is the RDS. Only 2 seconds where spent decrypting
then the last 6 seconds where spent waiting for receiver to catch up.
If we created a larger database instance, this time could easily disappear.
