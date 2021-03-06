job "publish-data" {
    datacenters = ["NOMAD_DATA_CENTER"]
        constraint {
    }
     update {
          stagger = "10s"
          max_parallel = 1
  }
    group "dp-publish-pipeline" {
        task "publish-data" {
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
                S3_URL = "S3_CONTENT_URL"
                S3_SECURE = "S3_SECURE_FLAG"
                S3_BUCKET = "S3_CONTENT_BUCKET"
                UPSTREAM_S3_BUCKET = "COLLECTION_S3_BUCKET"
                UPSTREAM_S3_URL = "COLLECTION_S3_URL"
                UPSTREAM_S3_SECURE = "COLLECTION_S3_SECURE"
                HEALTHCHECK_ADDR = ":${NOMAD_PORT_http}"
                HUMAN_LOG = "HUMAN_LOG_FLAG"
            }
            driver = "exec"
            config {
                command = "local/bin/publish-data"
                args = []
            }
            resources {
                cpu = 450
                memory = 300
                network {
                    port "http" {}
                }
            }
            service {
                port = "http"
                check {
                    type     = "http"
                    path     = "HEALTHCHECK_ENDPOINT"
                    interval = "10s"
                    timeout  = "2s"
                }
            }
        }
  }
}
