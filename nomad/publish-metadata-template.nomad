job "publish-metadata" {
    datacenters = ["NOMAD_DATA_CENTER"]
        constraint {
    }
     update {
          stagger = "10s"
          max_parallel = 1
  }
    group "dp-publish-pipeline" {
        task "publish-metadata" {
              artifact {
                        source = "S3_TAR_FILE_LOCATION"
                        // The Following options are needed if no IAM roles are provided
                        // options {
                        // aws_access_key_id = ""
                        // aws_access_key_secret = ""
                        // }
             }
            env {
                KAFKA_ADDR = "KAFKA_ADDRESS"
                S3_URL = "S3_CONTENT_URL"
                S3_BUCKET = "S3_CONTENT_BUCKET"
                UPSTREAM_S3_BUCKET = "UPSTREAM_S3_CONTENT_BUCKET"
                UPSTREAM_S3_URL = "UPSTREAM_S3_CONTENT_URL"
            }
            driver = "exec"
            config {
                command = "publish-metadata"
                args = []
            }
            resources {
                cpu = 450
                memory = 300
            }
        }
  }
}
