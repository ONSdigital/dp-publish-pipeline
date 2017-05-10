job "publish-deleter" {
    datacenters = ["NOMAD_DATA_CENTER"]
        constraint {
    }
     update {
          stagger = "10s"
          max_parallel = 1
  }
    group "dp-publish-pipeline" {
        task "publish-deleter" {
              artifact {
                        source = "s3::S3_TAR_FILE_LOCATION"
                        // The Following options are needed if no IAM roles are provided
                        // options {
                        // aws_access_key_id = ""
                        // aws_access_key_secret = ""
                        // }
             }
            env {
                KAFKA_ADDR = "KAFKA_ADDRESS"
                DB_ACCESS = "WEB_DB_ACCESS"
                ELASTIC_SEARCH_NODES = "ELASTIC_SEARCH_URL"
            }
            driver = "exec"
            config {
                command = "local/bin/publish-deleter"
                args = []
            }
            resources {
                cpu = 450
                memory = 300
            }
        }
  }
}
