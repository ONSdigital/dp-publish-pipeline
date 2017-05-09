job "publish-search-indexer" {
    datacenters = ["NOMAD_DATA_CENTER"]
        constraint {
    }
     update {
          stagger = "10s"
          max_parallel = 1
  }
    group "dp-publish-pipeline" {
        task "publish-search-indexer" {
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
                ELASTIC_SEARCH_NODES = "ELASTIC_SEARCH_URL"
            }
            driver = "exec"
            config {
                command = "publish-search-indexer"
                args = []
            }
            resources {
                cpu = 450
                memory = 300
            }
        }
  }
}
