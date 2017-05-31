job "publish-scheduler" {
    datacenters = ["NOMAD_DATA_CENTER"]
        constraint {
    }
     update {
          stagger = "10s"
          max_parallel = 1
  }
    group "dp-publish-pipeline" {
        task "publish-scheduler" {
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
                DB_ACCESS = "PUBLISH_DB_ACCESS"
                VAULT_ADDR = "VAULT_ADDRESS"
                VAULT_TOKEN = "SCHEDULER_VAULT_TOKEN"
                HEALTHCHECK_ADDR = ":${NOMAD_PORT_http}"
                HUMAN_LOG = "HUMAN_LOG_FLAG"
            }
            driver = "exec"
            config {
                command = "local/bin/publish-scheduler"
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
