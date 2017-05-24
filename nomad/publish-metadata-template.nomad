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
                        source = "s3::S3_TAR_FILE_LOCATION"
                        // The Following options are needed if no IAM roles are provided
                        // options {
                        // aws_access_key_id = ""
                        // aws_access_key_secret = ""
                        // }
             }
            env {
                KAFKA_ADDR = "KAFKA_ADDRESS"
                UPSTREAM_S3_BUCKET = "COLLECTION_S3_BUCKET"
                UPSTREAM_S3_URL = "COLLECTION_S3_URL"
                UPSTREAM_S3_ACCESS_KEY = "COLLECTION_S3_ACCESS_KEY"
                UPSTREAM_S3_SECRET_ACCESS_KEY = "COLLECTION_S3_SECRET_KEY"
                HEALTHCHECK_PORT = "${NOMAD_PORT_http}"
            }
            driver = "exec"
            config {
                command = "local/bin/publish-metadata"
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
                port = "${NOMAD_PORT_http}"
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
