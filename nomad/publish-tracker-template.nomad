job "publish-tracker" {
    datacenters = ["NOMAD_DATA_CENTER"]
        constraint {
    }
     update {
          stagger = "10s"
          max_parallel = 1
  }
    group "dp-publish-pipeline" {
        task "publish-tracker" {
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
            }
            driver = "exec"
            config {
                command = "local/bin/publish-tracker"
                args = []
            }
            resources {
                cpu = 450
                memory = 300
            }
        }
  }
}
