## Content API

A API used to get data from published meta and static files from different
resource.

### Environment variables
* `S3_URL` defaults to "localhost:4000"
* `S3_ACCESS_KEY` defaults to "1234"
* `S3_SECRET_ACCESS_KEY` defaults to "1234"
* `S3_BUCKET` defaults to "content"
* `DB_ACCESS` defaults to "user=dp dbname=dp sslmode=disable"
* `GENERATOR_URL` defaults to "localhost:8092"
* `PORT` defaults to "8082"

### Interfaces

#### Data
Route : Get /data

Parameters
* uri : location of meta document in the datastore

Errors
* 404 : meta document not found

#### Resource
Route : Get /resource

Parameters
* uri : location of meta document in the S3

Errors
* 404 : S3 file not found

#### Parent
Route : Get /parent

Hard coded value. See static-parent.json

#### Taxonomy
Route : Get /taxonomy

Hard coded value. See static-taxonomy.json
